package test.wikiparser;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Stack;

/**
 * /mediawiki
 * /mediawiki/page
 * /mediawiki/page/id
 * /mediawiki/page/ns
 * /mediawiki/page/redirect
 * /mediawiki/page/restrictions
 * /mediawiki/page/revision
 * /mediawiki/page/revision/comment
 * /mediawiki/page/revision/contributor
 * /mediawiki/page/revision/contributor/id
 * /mediawiki/page/revision/contributor/ip
 * /mediawiki/page/revision/contributor/username
 * /mediawiki/page/revision/format
 * /mediawiki/page/revision/id
 * /mediawiki/page/revision/minor
 * /mediawiki/page/revision/model
 * /mediawiki/page/revision/parentid
 * /mediawiki/page/revision/sha1
 * /mediawiki/page/revision/text
 * /mediawiki/page/revision/timestamp
 * /mediawiki/page/title
 * /mediawiki/siteinfo
 * /mediawiki/siteinfo/base
 * /mediawiki/siteinfo/case
 * /mediawiki/siteinfo/generator
 * /mediawiki/siteinfo/namespaces
 * /mediawiki/siteinfo/namespaces/namespace
 * /mediawiki/siteinfo/sitename
 */
public class WikiArchiveParser
{
  public static void main(String[] args)
    throws ParserConfigurationException, SAXException, IOException
  {
    if (args.length < 2) {
      System.out.println("usage WikiArchiveParser <file> <targetDir>");

      System.exit(1);
    }

    File in = new File(args[0]);
    if (!in.exists())
      throw new IllegalArgumentException(String.format(
        "file %1$s does not exist",
        in));

    File targetDir = new File(args[1]);
    if (!targetDir.exists() && !targetDir.mkdirs())
      throw new IllegalStateException(String.format(
        "directory %1$s can not be created",
        targetDir));

    SAXParserFactory factory = SAXParserFactory.newInstance();
    SAXParser parser = factory.newSAXParser();

    parser.parse(in, new ArticleHandler(targetDir, 20));
  }
}

class ArticleHandler extends DefaultHandler
{
  Stack<String> _stack = new Stack<>();

  StringBuilder _id;
  StringBuilder _title;
  StringBuilder _text;

  File _targetDir;
  long _limit;

  public ArticleHandler(File targetDir, long limit)
  {
    _targetDir = targetDir;

    _limit = limit;
  }

  @Override
  public void startElement(String uri,
                           String localName,
                           String qName,
                           Attributes attributes) throws SAXException
  {
    String path;
    if (_stack.size() > 0)
      path = _stack.peek();
    else
      path = "";

    path = path + '/' + qName;

    _stack.push(path);

    if ("/mediawiki/page".equals(path)) {
      _id = new StringBuilder();
      _title = new StringBuilder();
      _text = new StringBuilder();
    }
  }

  @Override
  public void endElement(String uri, String localName, String qName)
    throws SAXException
  {
    String path = _stack.pop();

    if ("/mediawiki/page".equals(path)) {
      writeFiles();

      _id = null;
      _title = null;
      _text = null;
    }
  }

  @Override
  public void characters(char[] ch, int start, int length)
    throws SAXException
  {
    String path = _stack.peek();

    if ("/mediawiki/page/id".equals(path)) {
      _id.append(ch, start, length);
    }
    else if ("/mediawiki/page/revision/text".equals(path)) {
      _text.append(ch, start, length);
    }
    else if ("/mediawiki/page/title".equals(path)) {
      _title.append(ch, start, length);
    }
  }

  private void writeFiles() throws SAXException
  {
    String cleanText = clean();

    if (cleanText.length() < 100)
      return;

    int id = Integer.parseInt(_id.toString());

    int bucket = id / 100;

    File dir = new File(_targetDir, Integer.toString(bucket));
    dir.mkdirs();

    File file = new File(dir, _id.toString() + ".txt");

    try (FileOutputStream out = new FileOutputStream(file);
         OutputStreamWriter writer
           = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {

      writer.write(clean());

      writer.flush();
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
      e.fillInStackTrace();
    } finally {
      if (--_limit == 0) {
        throw new SAXException("parse limit is reached");
      }
    }
  }

  private String clean()
  {
    StringBuilder temp = new StringBuilder();
    char[] chars = new char[_text.length()];

    _text.getChars(0, _text.length(), chars, 0);

    for (int i = 0; i < chars.length; i++) {
      char c = chars[i];
      switch (c) {
      case '{': {//{{xxx{xx}}}
        for (int x = 1; x > 0 && i + 1 < chars.length; ) {
          c = chars[++i];
          if (c == '{')
            x++;
          else if (c == '}')
            x--;
        }

        break;
      }
      case '[': {
        for (int x = 1; x > 0 && i + 1 < chars.length; ) {
          c = chars[++i];
          if (c == '[')
            x++;
          else if (c == ']')
            x--;
        }

        break;
      }
      case '*':
      case '#': {
        if (i > 1 && chars[i - 1] == '\n') {
          for (; i < chars.length && chars[i] != '\n'; i++) ;
        }

        break;
      }
      case '\'': {
        break;
      }
      default: {
        temp.append(c);
      }
      }
    }

    return temp.toString();
  }

  @Override
  public void endDocument() throws SAXException
  {

  }
}