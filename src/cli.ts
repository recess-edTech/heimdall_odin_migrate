#!/usr/bin/env node

import { program } from "commander";
import { initWithNpm,} from "./utils/initFiles";
import { promptForProjectName } from "./modules/promptMods";


program
  .name("matsumi")
  .description("A CLI tool for initializing repositories with npm or pnpm")
  .version("1.0.0");

program
  .command("init")
  .description("Initialize a new repository")
  .option("-n, --npm", "Initialize with npm")
  .option("-h, --help", "Display help for the init command")
  .argument("[project-name]", "Name of the project")
  .action(async (projectName, options) => {
    if (options.help) {
      console.log("Usage: matsumi init [options] [project-name]");
      console.log();
      console.log("Options:");
      console.log("  -n, --npm  Initialize with npm");
      console.log("  -h, --help  Display help for the init command");
      console.log();
      console.log("Arguments:");
      console.log("  [project-name]  Name of the project");
      return;
    }

    if (!projectName) {
      console.log("No project name provided please enter the project name:");
      projectName = await promptForProjectName();
    }

    if (options.npm) {
      await initWithNpm(projectName);
    } else {
      console.error("Please specify the --npm option");
    }
  });

program.parse(process.argv);
