package org.comp7705.client;

import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.comp7705.client.entity.CmdParam;
import org.comp7705.client.services.ClientService;
import org.comp7705.client.services.impl.ClientServiceImpl;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;


@Slf4j
public class Client {

    private static final String COMMANDS_HELP
            = "h, help: List all the commands available for the client. For help of each command, use <COMMAND> -h" + System.lineSeparator()
            + "get: Download a remote file to a local location" + System.lineSeparator()
            + "add: Upload a local file to a remote location" + System.lineSeparator()
            + "mkdir: Make a new directory on remote location" + System.lineSeparator()
            + "remove: Remove a file or a directory on the remote" + System.lineSeparator()
            + "list: List all files and directories under the remote path" + System.lineSeparator()
            + "move: Move a file or a directory on the remote to another location" + System.lineSeparator()
            + "rename: Rename a file or a directory on the remote to another name" + System.lineSeparator()
            + "stat: Retrieve the status of a file or a directory on the remote" + System.lineSeparator()
            + "batch: Execute a sequence of commands in a batch fil" + System.lineSeparator();

    public static void main(String[] args) {
        ClientService clientService = new ClientServiceImpl();
        if ("batch".equals(args[0])) {
            CmdParam cmdParam = getSrc(args);
            parseBatch(cmdParam.getSrc(), clientService);
        } else {
            parseCommand(args, clientService);
        }
    }

    public static void parseBatch(String src, ClientService clientService) {
        try (FileInputStream in = new FileInputStream(src);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String cmd = reader.readLine();
            while(cmd != null) {
                String[] args = cmd.split("\\s+");
                parseCommand(args, clientService);
                cmd = reader.readLine();
            }
        } catch (IOException e) {
            log.error(String.format("Failed to read %s because of IOException", src), e);
            System.out.printf("\033[1;31mIO Exception %s\033[0m\n", e);
        }
    }

    public static void parseCommand(String[] args, ClientService clientService) {
        log.info(Joiner.on(' ').join(args));
        CmdParam cmdParam;
        switch (args[0]) {
            case "h":
            case "help":
            case "-h":
            case "--help":
                System.out.println(COMMANDS_HELP);
                break;
            case "get":
                cmdParam = getSrcAndDes(args);
                clientService.get(cmdParam.getSrc(), cmdParam.getDes());
                break;
            case "add":
                cmdParam = getSrcAndDes(args);
                clientService.add(cmdParam.getSrc(), cmdParam.getDes());
                break;
            case "mkdir":
                cmdParam = getDes(args);
                clientService.mkdir(cmdParam.getDesPath(), cmdParam.getDesName());
                break;
            case "remove":
                cmdParam = getDes(args);
                clientService.remove(cmdParam.getDes());
                break;
            case "list":
                cmdParam = getDesAndLatest(args);
                clientService.list(cmdParam.getDes(), cmdParam.isLatest());
                break;
            case "move":
                cmdParam = getSrcAndDes(args);
                clientService.move(cmdParam.getSrc(), cmdParam.getDes());
                break;
            case "rename":
                cmdParam = getSrcAndDes(args);
                clientService.rename(cmdParam.getSrc(), cmdParam.getDes());
                break;
            case "stat":
                cmdParam = getDesAndLatest(args);
                clientService.stat(cmdParam.getDes(), cmdParam.isLatest());
                break;
            default:
                System.out.println("No such command!!");
                System.out.println(COMMANDS_HELP);
        }
    }

    private static CmdParam getDes(String[] args) {
        Options options = new Options();
        options.addOption("d", "des", true, "The destination of the "+args[0]);
        options.addOption("h", "help", false, "Print the usage of the command");
        CommandLine line = parseOptions(options, args);
        if (!line.hasOption('d')) {
            log.warn("Missing argument: des");
            printHelp(options, args);
            System.exit(0);
        }
        CmdParam cmdParam = new CmdParam();
        cmdParam.setDes(line.getOptionValue('d'));
        return cmdParam;
    }

    private static CmdParam getSrc(String[] args) {
        Options options = new Options();
        options.addOption("s", "src", true, "The source of the "+args[0]);
        options.addOption("h", "help", false, "Print the usage of the command");
        CommandLine line = parseOptions(options, args);
        if (!line.hasOption('s')) {
            log.warn("Missing argument: src");
            printHelp(options, args);
            System.exit(0);
        }
        CmdParam cmdParam = new CmdParam();
        cmdParam.setSrc(line.getOptionValue('s'));
        return cmdParam;
    }

    private static CmdParam getSrcAndDes(String[] args) {
        Options options = new Options();
        options.addOption("s", "src", true, "The source of the "+args[0]);
        options.addOption("d", "des", true, "The destination of the "+args[0]);
        options.addOption("h", "help", false, "Print the usage of the command");
        CommandLine line = parseOptions(options, args);
        if (!line.hasOption('s')) {
            log.warn("Missing argument: src");
            printHelp(options, args);
            System.exit(0);
        }
        if (!line.hasOption('d')) {
            log.warn("Missing argument: des");
            printHelp(options, args);
            System.exit(0);
        }
        CmdParam cmdParam = new CmdParam();
        cmdParam.setSrc(line.getOptionValue('s'));
        cmdParam.setDes(line.getOptionValue('d'));
        return cmdParam;
    }

    private static CmdParam getDesAndLatest(String[] args) {
        Options options = new Options();
        options.addOption("l", "latest", false, "Get the latest version of the "+args[0]);
        options.addOption("d", "des", true, "The destination of the "+args[0]);
        options.addOption("h", "help", false, "Print the usage of the command");
        CommandLine line = parseOptions(options, args);
        if (!line.hasOption('d')) {
            log.warn("Missing argument: des");
            printHelp(options, args);
            System.exit(0);
        }
        CmdParam cmdParam = new CmdParam();
        cmdParam.setLatest(line.hasOption('l'));
        cmdParam.setDes(line.getOptionValue('d'));
        return cmdParam;
    }

    private static CommandLine parseOptions(Options opts, String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLine line = null;
        try {
            line = parser.parse(opts, Arrays.copyOfRange(args, 1, args.length));
        } catch (ParseException e) {
            log.warn(e.getMessage());
            System.exit(0);
        }
        if (line.hasOption('h')) {
            printHelp(opts, args);
            System.exit(0);
        }
        return line;
    }

    private static void printHelp(Options opts, String[] args) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(args[0], opts);
    }
}
