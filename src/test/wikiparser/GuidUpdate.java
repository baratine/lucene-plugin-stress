package test.wikiparser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Properties;

public class GuidUpdate implements FileVisitor<Path>
{
  private final Path _dest;
  private final Path _src;
  private Path _dst;
  private Properties _properties;

  public GuidUpdate(Path src, Path dest) throws IOException
  {
    _src = src;

    _dest = Files.createDirectories(dest);

    _properties = new Properties();

    try (Reader reader
           = Files.newBufferedReader(src.resolve("wiki/query.properties"),
                                     StandardCharsets.UTF_8)) {
      _properties.load(reader);
    }
  }

  public void execute() throws IOException
  {
    Files.walkFileTree(_src, this);

    Path path = _dest.resolve("wiki/query.properties");

    Files.createDirectories(path.getParent());

    try (Writer out = Files.newBufferedWriter(path)) {
      _properties.store(out, null);
    }
  }

  @Override
  public FileVisitResult visitFile(final Path file, BasicFileAttributes attrs)
    throws IOException
  {
    assert attrs.size() <= Integer.MAX_VALUE;

    if (attrs.isDirectory())
      return FileVisitResult.CONTINUE;
    else if (!file.getFileName().toString().endsWith(".txt"))
      return FileVisitResult.CONTINUE;

    final Path path = file.getFileName();

    final String name = path.toString();

    final String bareName = name.substring(0, name.lastIndexOf('.'));

    final Path destDir
      = _dest.resolve("wiki").resolve(file.getParent().getFileName());

    Files.createDirectories(destDir);

    Path destFile = destDir.resolve(path);

    byte[] buffer = new byte[(int) attrs.size()];

    try (InputStream in = Files.newInputStream(file);
         OutputStream out = Files.newOutputStream(destFile)) {

      int l = in.read(buffer);
      assert l != buffer.length;

      String uid = new String(buffer, buffer.length - 38, 36);
      uid = uid.replace('-', 'A');

      Files.createDirectories(destDir);

      //out.write(buffer, 0, buffer.length - 38);

      byte[] newBytes = new byte[buffer.length];
      int j = 0;
      for (int i = 0; i < buffer.length - 38; i++) {
        byte b = buffer[i];

        switch (b) {
        case '/':
        case '\\':
        case '<':
        case '>':
          break;
        default:
          newBytes[j++] = b;
        }

      }

      out.write(newBytes, 0, j);

      out.write(uid.getBytes());
      out.write(" .".getBytes());

      _properties.put(bareName, uid);
    }

    return FileVisitResult.CONTINUE;
  }

  @Override
  public FileVisitResult visitFileFailed(Path file, IOException exc)
    throws IOException
  {
    return FileVisitResult.TERMINATE;
  }

  @Override
  public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
    throws IOException
  {
    return FileVisitResult.CONTINUE;
  }

  @Override
  public FileVisitResult postVisitDirectory(Path dir, IOException exc)
    throws IOException
  {
    return FileVisitResult.CONTINUE;
  }

  public static void main(String[] args) throws IOException
  {
    FileSystem fs = FileSystems.getDefault();
    Path src = fs.getPath(args[0]);
    Path dest = fs.getPath(args[1]);

    new GuidUpdate(src, dest).execute();
  }
}
