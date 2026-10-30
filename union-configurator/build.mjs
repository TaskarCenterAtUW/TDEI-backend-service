// Builds the configurator from src/:
//   dist/union-configurator.js     the ES module the portal imports (mount, getConfig, setConfig, destroy)
//   dist/union-configurator.html   the standalone page, module inlined
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const { version } = JSON.parse(readFileSync(join(here, 'package.json'), 'utf8'));
const module = readFileSync(join(here, 'src/union-configurator.js'), 'utf8').replace("'__VERSION__'", `'${version}'`);
const page = readFileSync(join(here, 'src/standalone.html'), 'utf8')
  .replace('/*__MODULE__*/', () => module.replace('export function mount', 'function mount'));

mkdirSync(join(here, 'dist'), { recursive: true });
writeFileSync(join(here, 'dist/union-configurator.js'), module);
writeFileSync(join(here, 'dist/union-configurator.html'), page);
console.log(`union-configurator ${version}: dist/union-configurator.js, dist/union-configurator.html`);
