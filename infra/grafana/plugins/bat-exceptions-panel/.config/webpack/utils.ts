import fs from 'fs';
import os from 'os';
import path from 'path';
import process from 'process';
import { glob } from 'glob';
import { SOURCE_DIR } from './constants';

export function isWSL() {
  if (process.platform !== 'linux') {
    return false;
  }

  if (os.release().toLowerCase().includes('microsoft')) {
    return true;
  }

  try {
    return fs.readFileSync('/proc/version', 'utf8').toLowerCase().includes('microsoft');
  } catch {
    return false;
  }
}

export function getPackageJson() {
  return require(path.resolve(process.cwd(), 'package.json'));
}

export function getPluginJson() {
  return require(path.resolve(process.cwd(), SOURCE_DIR, 'plugin.json'));
}

export function hasReadme() {
  return fs.existsSync(path.resolve(process.cwd(), SOURCE_DIR, 'README.md'));
}

export async function getEntries(): Promise<Record<string, string>> {
  const pluginsJson = await glob('**/src/**/plugin.json', { absolute: true });

  const plugins = await Promise.all(
    pluginsJson.map(async (pluginJson) => {
      const folder = path.dirname(pluginJson);
      return glob(`${folder}/module.{ts,tsx,js,jsx}`, { absolute: true });
    })
  );

  return plugins.reduce<Record<string, string>>((result, modules) => {
    return modules.reduce((innerResult, module) => {
      const pluginPath = path.relative(process.cwd(), path.dirname(module)).replace(/\\/g, '/');
      const pluginName = pluginPath === SOURCE_DIR ? 'module' : pluginPath.replace(`${SOURCE_DIR}/`, '');

      innerResult[pluginName] = module;
      return innerResult;
    }, result);
  }, {});
}
