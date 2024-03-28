#!/usr/bin/env node
import { exec } from 'child_process';
import { promisify } from 'util';
import * as readline from 'readline';
import * as path from 'path';

const execPromise = promisify(exec);
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

export async function initWithNpm(projectName: string) {
  try {
    // Check if npm is installed
    await execPromise('npm -v');
  } catch (error) {
    console.log('npm is not installed, installing npm');
    try {
      await execPromise('npm install npm@latest -g');
      console.log('npm installed successfully');
    } catch (error) {
      console.error(error);
      return;
    }
  }

  try {
    const projectDir = path.join(process.cwd(), projectName);
    console.log(`Creating project directory: ${projectDir}`);
    const createFilesCommand = `
      mkdir -p ${projectDir} &&
      cd ${projectDir} &&
      npm init -y --scope=@${projectName} &&
      if [ ! -d "src" ]; then
        mkdir src
        mkdir src/utils
      fi &&
      touch src/index.ts &&
      touch src/cli.ts &&
      touch src/utils/initFiles.ts &&
      touch src/utils/parseArgs.ts
    `;

    console.log('Initializing repository using npm init -y');
    await execPromise(createFilesCommand);
    console.log('npm init success');
    console.log('Creating necessary files');
    console.log('Files created successfully');

    await execPromise('git init', { cwd: projectDir });
    console.log('git init success');

    console.log('Project successfully initialized!');
    process.exit(0);
  } catch (error) {
    console.error(error);
  }
}

export async function promptForProjectName(): Promise<string> {
  return new Promise((resolve) => {
    rl.question('Enter project name: ', (name) => {
      rl.close();
      resolve(name);
    });
  });
}