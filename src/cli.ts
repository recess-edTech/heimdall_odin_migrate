#!/usr/bin/env node
import { program } from "commander";

program
  .command("test init")
  .description("innit test")
  .option("-f, --force", "force init")
  .action((options) => {
    import("chalk").then((chalk) => {
      console.error(chalk.default.green("init test", options.force));
    });
  });


program.parse(process.argv);