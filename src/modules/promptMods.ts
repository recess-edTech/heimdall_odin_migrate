import * as readline from 'readline';
import { exec } from 'child_process';
import { promisify } from 'util';

const execPromise = promisify(exec);
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

export async function promptForProjectName(): Promise<string> {
  return new Promise((resolve) => {
    rl.question('Enter project name: ', (name) => {
      rl.close();
      resolve(name);
    });
  });
}

async function prompt(question: string, action: (projectDir: string) => Promise<void>, projectDir: string) {
  return new Promise<void>((resolve) => {
    rl.question(question, async (answer) => {
      rl.close();
      if (answer === "y") {
        await action(projectDir);
      }
      resolve();
    });
  });
}

export function promptForGit(projectDir: string) {
  return prompt(
    "Do you want to initialize a git repository? (y/n): ",
    async (projectDir) => {
      console.log("Initializing git repository");
      await execPromise("git init", { cwd: projectDir });
    },
    projectDir
  );
}

export function promptForVSCode(projectDir: string) {
  return prompt(
    "Do you want to open the project with VSCode? (y/n): ",
    async (projectDir) => {
      console.log("Opening project with VSCode");
      await execPromise("code .", { cwd: projectDir });
    },
    projectDir
  );
}