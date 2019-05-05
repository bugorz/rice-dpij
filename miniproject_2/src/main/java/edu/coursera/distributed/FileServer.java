package edu.coursera.distributed;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A basic and very limited implementation of a file server that responds to GET
 * requests from HTTP clients.
 */
public final class FileServer {
    /**
     * Main entrypoint for the basic file server.
     *
     * @param socket Provided socket to accept connections on.
     * @param fs A proxy filesystem to serve files from. See the PCDPFilesystem
     *           class for more detailed documentation of its usage.
     * @throws IOException If an I/O error is detected on the server. This
     *                     should be a fatal error, your file server
     *                     implementation is not expected to ever throw
     *                     IOExceptions during normal operation.
     */
    public void run(final ServerSocket socket, final PCDPFilesystem fs)
            throws IOException {

        Pattern p = Pattern.compile("(.+)GET\\s+([\\w/]+)\\s+HTTP/1.1");

        /*
         * Enter a spin loop for handling client requests to the provided
         * ServerSocket object.
         */
        while (true) {

            // 1) Use socket.accept to get a Socket object
            Socket socketObject = socket.accept();

            /*
             * 2) Using Socket.getInputStream(), parse the received HTTP
             * packet. In particular, we are interested in confirming this
             * message is a GET and parsing out the path to the file we are
             * GETing. Recall that for GET HTTP packets, the first line of the
             * received packet will look something like:
             *
             *     GET /path/to/file HTTP/1.1
             */
            InputStream inputStream = socketObject.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));


            final String inputLine = reader.readLine();

            if (inputLine == null) continue;

            final String path = inputLine.split("\\s")[1];

            /*
             * 3) Using the parsed path to the target file, construct an
             * HTTP reply and write it to Socket.getOutputStream(). If the file
             * exists, the HTTP reply should be formatted as follows:
             *
             *   HTTP/1.0 200 OK\r\n
             *   Server: FileServer\r\n
             *   \r\n
             *   FILE CONTENTS HERE\r\n
             *
             * If the specified file does not exist, you should return a reply
             * with an error code 404 Not Found. This reply should be formatted
             * as:
             *
             *   HTTP/1.0 404 Not Found\r\n
             *   Server: FileServer\r\n
             *   \r\n
             *
             * Don't forget to close the output stream.
             */

            final OutputStream outputStream = socketObject.getOutputStream();
            final PCDPPath pcdpPath = new PCDPPath(path);
            final PrintStream printStream = new PrintStream(outputStream);

            String fileOutput = fs.readFile(pcdpPath);
            if (fileOutput != null) {
                printStream.print("HTTP/1.0 200 OK\r\nServer: FileServer\r\n\r\n");
                printStream.print(fileOutput + "\r\n");
            } else {
                printStream.print("HTTP/1.0 404 Not Found\r\nServer: FileServer\r\n\r\n");
            }

            printStream.flush();
            printStream.close();
        }
    }
}
