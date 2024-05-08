import { Queue } from 'async-await-queue';
import config from 'config';
import { Corellium } from '@corellium/corellium-api';
import Fastify from 'fastify';
import FastifyMultipart from '@fastify/multipart';
import fs from 'fs';
import HttpErrors from 'http-errors';
import { createTokenAuth } from '@octokit/auth-token';
import { Octokit } from '@octokit/core';
import stream from 'stream';

const {
  BadRequest,
  Forbidden,
  NotFound,
  Unauthorized,
} = HttpErrors;

const RUN_TIMEOUT = config.get('Corellium.timeout');
const AUTHORIZED_OWNERS = new Set(config.get('GitHub.authorized_owners'));

let corellium = null;
const globalSerialQueue = new Queue();
const deviceQueues = new Map();

const fastify = Fastify({
  trustProxy: true,
  logger: true
});

fastify.register(FastifyMultipart, {
  limits: {
    headerPairs: 20,
    fields: 3,
    fieldNameSize: 16,
    fieldSize: 1024,
    files: 1,
    fileSize: 128 * 1024 * 1024
  }
});

async function start() {
  try {
    await fastify.listen({ host: '::1', port: 3000 });
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
}

fastify.route({
  method: 'POST',
  url: '/devices/:device',
  handler: async (request, reply) => {
    const deviceId = request.params.device;

    const files = await request.saveRequestFiles();
    const asset = files.find(f => f.fieldname === 'asset');
    if (asset === undefined)
      throw new BadRequest('missing asset file');

    const { fields } = asset;
    const script = fields.script?.value?.replace(/\r/g, '') ?? null;
    const marker = fields.marker?.value ?? null;
    const token = fields.token?.value ?? null;
    if (script === null)
      throw new BadRequest('missing script field');
    if (marker === null)
      throw new BadRequest('missing marker field');
    if (token === null)
      throw new BadRequest('missing token field');

    await authenticate(token);

    const output = new stream.Readable();
    output._read = () => {};

    reply.header('Content-Type', 'text/plain; charset=utf-8');
    uploadAndRun({ deviceId, token, asset, script, marker, output });

    return reply.send(output);
  },
});

async function uploadAndRun({ deviceId, asset, script, marker, output }) {
  let state = 'open';

  emit('[*] Preparing\n');

  try {
    let project = null;
    await globalSerialQueue.run(async () => {
      if (corellium === null) {
        const { endpoint, username, password } = config.get('Corellium');
        const c = new Corellium({ endpoint, username, password });
        emit('[*] Logging in\n');
        await c.login();
        corellium = c;
      }

      const projectName = config.get('Corellium.project');
      emit('[*] Resolving project\n');
      const p = await corellium.projectNamed(projectName);
      if (p === undefined)
        throw new ServiceUnavailable(`project '${projectName}' not found`);
      project = p;
    });

    let deviceQueue = deviceQueues.get(deviceId);
    if (deviceQueue === undefined) {
      deviceQueue = new Queue();
      deviceQueues.set(deviceId, deviceQueue);
    }

    emit('[*] Waiting for device to become available\n');

    return await deviceQueue.run(async () => {
      emit('[*] Locating instance\n');
      const instance = (await project.instances()).find(candidate => candidate.name == deviceId);
      if (instance === undefined) {
        deviceQueues.delete(deviceId);
        throw new NotFound(`device '${deviceId}' not found`);
      }

      if (instance.state !== 'on') {
        emit('[*] Starting instance\n');
        await instance.start();
        emit('[*] Waiting for it to boot\n');
        await instance.waitForState('on');
      }

      emit('[*] Connecting to agent\n');
      const agent = await instance.agent();
      await agent.ready();

      let assetPath = null;
      let logPath = null;
      let logOffset = 0;

      try {
        emit('[*] Creating remote temporary file\n');
        assetPath = await agent.tempFile();
        emit('[*] Starting upload\n');
        await agent.upload(assetPath, fs.createReadStream(asset.filepath), total => {
          emit(`[*] Uploaded ${total} / ${asset.file.bytesRead} bytes\n`);
        });

        const wrapperScriptLines = [
          'SCRIPT_LOG=$(mktemp)',
          'cat << EOF | setsid sh > /dev/null 2>&1 &',
            `export ASSET_PATH=${assetPath}`,
            '(',
              'set -ex',
              script.replace(/\$/g, '\\$'),
            ') > $SCRIPT_LOG 2>&1',
            `echo "${marker}\\$?" >> $SCRIPT_LOG`,
          'EOF',
          'echo $!',
          'echo $SCRIPT_LOG',
        ];
        if (instance.type === 'android') {
          wrapperScriptLines.splice(0, 0, 'export TMPDIR=/data/local/tmp');
        }

        emit(`[*] Running script\n\n`);
        const spawnResult = await agent.shellExec(wrapperScriptLines.join('\n') + '\n');
        const lines = spawnResult.output.split('\n');
        const pid = parseInt(lines[0]);
        logPath = lines[1];

        let timer = null;
        let innerPid = null;
        const timeout = new Promise((resolve, reject) => {
          timer = setTimeout(async () => reject(new Error('Timed out')), RUN_TIMEOUT);
        });
        timeout.catch(e => {
          agent.shellExec(`kill -KILL -$(ps -A -o pid= -o pgid= | awk '{ if ($1 == ${pid}) print $2; }')`)
            .catch(() => {});
        });

        try {
          while (await Promise.race([poll(pid), timeout]) === 'alive') {
          }
        } finally {
          clearTimeout(timer);
        }

        await consumeLog();
      } finally {
        const tempFiles = [assetPath, logPath].filter(p => p !== null);
        if (tempFiles.length > 0) {
          agent.shellExec('rm ' + tempFiles.join(' ')).catch(() => {});
        }
      }

      async function poll(pid) {
        await sleep(3000);

        const pollResult = await agent.shellExec(`kill -0 ${pid}`);
        if (pollResult['exit-status'] !== 0)
          return 'dead';

        await consumeLog();

        return 'alive';
      }

      async function consumeLog() {
        const data = await consumeStream(await agent.download(logPath));

        const endOffset = data.length;
        if (endOffset === logOffset)
          return;
        const slice = data.subarray(logOffset, endOffset);
        logOffset = endOffset;

        emit(slice);
      }
    });
  } catch (e) {
    emit(`[!] ${e.stack}\n`);
  } finally {
    emit(null);
  }

  function emit(value) {
    if (state !== 'open')
      return;
    output.push(value);
    if (value === null)
      state = 'closed';
  }
}

async function authenticate(token) {
  let octokit, authentication;
  try {
    octokit = new Octokit({ auth: token });

    const auth = createTokenAuth(token);
    authentication = await auth();
  } catch (e) {
    throw new Unauthorized('invalid token');
  }

  if (authentication.tokenType === 'installation') {
    let repos;
    try {
      repos = await octokit.request('GET /installation/repositories');
    } catch (e) {
      throw new Unauthorized('invalid token');
    }

    if (!repos.data.repositories.some(repo => AUTHORIZED_OWNERS.has(repo.owner.login))) {
      throw new Forbidden('not among authorized owners');
    }
  } else {
    let orgs;
    try {
      orgs = await octokit.request('GET /user/orgs');
    } catch (e) {
      throw new Unauthorized('invalid token');
    }

    if (!orgs.data.some(org => AUTHORIZED_OWNERS.has(org.login))) {
      throw new Forbidden('not among authorized owners');
    }
  }
}

function sleep(duration) {
  return new Promise(resolve => {
    setTimeout(resolve, duration);
  });
}

function consumeStream(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on('data', chunk => chunks.push(chunk));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
    stream.on('error', e => reject(e));
  });
}

start();
