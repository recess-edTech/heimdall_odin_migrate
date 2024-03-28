#!/usr/bin/env node
import { exec } from 'child_process';
import { promisify } from 'util';
import * as readline from 'readline';
import * as path from 'path';
import { promptForGit, promptForVSCode } from '../modules/promptMods';

const execPromise = promisify(exec);
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function checkAndInstallPackage(packageName: string, installCommand: string) {
  try {
    await execPromise(`${packageName} -v`);
  } catch (error) {
    console.log(`${packageName} is not installed, installing ${packageName}`);
    try {
      await execPromise(installCommand);
      console.log(`${packageName} installed successfully`);
    } catch (error) {
      console.error(error);
      return;
    }
  }
}

async function initProjectWithPackage(packageManager: 'npm' | 'pnpm', projectName: string) {
  try {
    const projectDir = path.join(process.cwd(), projectName);
    console.log(`Creating project directory: ${projectDir}`);

    const createFilesCommand = `
      mkdir -p ${projectDir} &&
      cd ${projectDir} &&
      ${packageManager} init ${packageManager === 'npm' ? '-y' : ''} --scope=@${projectName} &&
      if [ ! -d "src" ]; then
        mkdir src
        mkdir src/utils
      fi &&
      touch src/index.ts &&
      touch src/cli.ts &&
      touch src/utils/initFiles.ts &&
      touch src/utils/parseArgs.ts
    `;

    console.log(`Initializing repository using ${packageManager} init`);
    await execPromise(createFilesCommand);
    console.log(`${packageManager} init success\nCreating files...\nFiles created successfully`);

    await promptForGit(projectDir);
    await promptForVSCode(projectDir);

    console.log('Project successfully initialized!');
    process.exit(0);
  } catch (error) {
    console.error(error);
  }
}

export async function initWithNpm(projectName: string) {
  await checkAndInstallPackage('npm', 'npm install npm@latest -g');
  await initProjectWithPackage('npm', projectName);
}

export async function initWithPnpm(projectName: string) {
  await checkAndInstallPackage('pnpm', 'sudo npm install pnpm@latest -g');
  await initProjectWithPackage('pnpm', projectName);
}