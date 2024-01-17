import Queue from 'async-await-queue';
import config from 'config';
import { Corellium } from '@corellium/corellium-api';
import Fastify from 'fastify';
import FastifyMultipart from 'fastify-multipart';
import HttpErrors from 'http-errors';
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
    fileSize: 32 * 1024 * 1024
  }
});

async function start() {
  try {
    await fastify.listen(3000, '::1');
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
}

fastify.route({
  method: 'POST',
  url: '/devices/:device',
  handler: async (request, reply) => {
    try {
      let project = null;
      await globalSerialQueue.run(async () => {
        if (corellium === null) {
          const { endpoint, username, password } = config.get('Corellium');
          const c = new Corellium({ endpoint, username, password });
          await c.login();
          corellium = c;
        }

        const projectName = config.get('Corellium.project');
        const p = await corellium.projectNamed(projectName);
        if (p === undefined)
          throw new ServiceUnavailable(`project '${projectName}' not found`);
        project = p;
      });

      const deviceId = request.params.device;
      let deviceQueue = deviceQueues.get(deviceId);
      if (deviceQueue === undefined) {
        deviceQueue = new Queue();
        deviceQueues.set(deviceId, deviceQueue);
      }

      return await deviceQueue.run(async () => {
        const instance = (await project.instances()).find(candidate => candidate.name == deviceId);
        if (instance === undefined) {
          deviceQueues.delete(deviceId);
          throw new NotFound(`device '${deviceId}' not found`);
        }

        if (instance.state !== 'on') {
          await instance.start();
          await instance.waitForState('on');
        }

        const agent = await instance.agent();
        await agent.ready();

        let token = null;
        let assetPath = null;
        let script = null;
        let marker = null;

        let logPath = null;
        let output = null;
        let logOffset = 0;

        try {
          const parts = request.parts();
          for await (const { fieldname, file, value } of parts) {
            switch (fieldname) {
              case 'token':
                token = value;
                break;
              case 'asset':
                if (file === undefined)
                  throw new BadRequest('asset must be a file');
                assetPath = await agent.tempFile();
                await agent.upload(assetPath, file);
                break;
              case 'script':
                if (value === undefined)
                  throw new BadRequest('script must be a field');
                script = value.replace(/\r/g, '');
                break;
              case 'marker':
                marker = value;
                break;
            }
          }
          if (token === null)
            throw new BadRequest('missing token field');
          if (assetPath === null)
            throw new BadRequest('missing asset file');
          if (script === null)
            throw new BadRequest('missing script field');
          if (marker === null)
            throw new BadRequest('missing marker field');

          await authenticate(token);

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

          const spawnResult = await agent.shellExec(wrapperScriptLines.join('\n') + '\n');
          const lines = spawnResult.output.split('\n');
          const pid = parseInt(lines[0]);
          logPath = lines[1];

          output = new stream.Readable();
          output._read = () => {};
          reply.send(output);

          try {
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
          } catch (e) {
          }
        } finally {
          output?.push(null);

          const tempFiles = [assetPath, logPath].filter(p => p !== null);
          if (tempFiles.length > 0) {
            agent.shellExec('rm ' + tempFiles.join(' ')).catch(() => {});
          }
        }

        async function poll(pid) {
          await sleep(1000);

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

          output.push(slice);
        }
      });
    } catch (e) {
      reply.code(500).send(e);
    }
  }
});

async function authenticate(token) {
  let repos;
  try {
    const octokit = new Octokit({ auth: token });
    repos = await octokit.request('GET /installation/repositories');
  } catch (e) {
    throw new Unauthorized('invalid token');
  }

  if (!repos.data.repositories.some(repo => AUTHORIZED_OWNERS.has(repo.owner.login))) {
    throw new Forbidden('not among authorized owners');
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
