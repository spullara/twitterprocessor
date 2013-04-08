package twitterprocessor;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.FlatMapper;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static java.util.stream.Collectors.groupingByConcurrent;
import static java.util.stream.Collectors.reducing;

/**
 * Read and process a feed of compressed tweets
 */
public class App {

  private static final int _1K = 1024;
  private static final int _128K = 128 * _1K;

  @Argument(alias = "f", description = "File or s3 url", required = true)
  private static String file;

  @Argument(alias = "a", description = "AWS properties file specifying accessKey and secretKey")
  private static File auth;

  @Argument(alias = "p", description = "Number of processor threads")
  private static Integer processors = 2;

  private static Logger squelchLog = Logger.getLogger("Squelched");

  public static void main(String[] args) throws IOException, URISyntaxException {
    try {
      Args.parse(App.class, args);
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      Args.usage(App.class);
      System.exit(1);
    }

    MappingJsonFactory jf = new MappingJsonFactory();

    FlatMapper<String, JsonNode> lineToJson = squelch((line, consumer) -> consumer.accept(jf.createParser(line).readValueAsTree()));

    Pattern noProtocol = Pattern.compile("^[A-Za-z0-9-]+[.]");

    ConcurrentLinkedQueue<String> bitlyList = new ConcurrentLinkedQueue<>();
    FlatMapper<String, String> toURL = squelch((link, consumer) -> {
      String host;
      URL url;
      if (noProtocol.matcher(link).find()) {
        url = new URL("http://" + link);
        host = url.getHost();
      } else {
        url = new URL(link);
        host = url.getHost();
      }
      if (host.equals("bit.ly")) {
        bitlyList.add(url.toString());
      } else {
        consumer.accept(host);
      }
    });

    FlatMapper<JsonNode, String> toLinks = (tweet, consumer) -> {
      JsonNode links = tweet.get("l");
      if (links != null) {
        links.forEach(link -> consumer.accept(link.textValue()));
      }
    };

    long start = System.currentTimeMillis();

    InputStream in;
    if (file.startsWith("s3://")) {
      if (auth == null) {
        throw new IllegalArgumentException("You must specify an auth properties file for AWS");
      } else {
        Properties aws = new Properties();
        aws.load(new FileInputStream(auth));
        URI url = new URI(file);
        GetObjectRequest get = new GetObjectRequest(url.getHost(), url.getPath().substring(1));
        AmazonS3Client client = new AmazonS3Client(new BasicAWSCredentials(aws.getProperty("accessKey"), aws.getProperty("secretKey")));
        S3Object object = client.getObject(get);
        in = object.getObjectContent();
      }
    } else {
      in = new FileInputStream(file);
    }

    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(in, _1K), "UTF-8"), _128K);
    AtomicInteger tweets = new AtomicInteger();
    AtomicInteger links = new AtomicInteger();
    Stream<String> lines = bufferedReader.lines();
    ConcurrentMap<String, Integer> map = lines.parallel()
            .peek(a -> {
              int i = tweets.incrementAndGet();
              if (i % 100000 == 0) {
                System.out.println("Processed " + i + " " + i / (System.currentTimeMillis() - start));
              }
            })
            .flatMap(lineToJson)
            .flatMap(toLinks)
            .peek(a -> links.incrementAndGet())
            .flatMap(toURL)
            .collect(groupingByConcurrent(u -> u, reducing(u -> 1, Integer::sum)));

    // Now resolve all the bit.ly URLs


    List<Map.Entry<String, Integer>> entries = new ArrayList<>(map.entrySet());
    entries.sort((e1, e2) -> e2.getValue() - e1.getValue());
    entries.stream().filter(entry -> entry.getValue() >= 10).forEach(entry -> {
      System.out.println(entry.getKey() + "," + entry.getValue());
    });

    System.out.println(entries.size() + " unique domains in " + links + " links from " + tweets + " tweets");
    System.out.println(System.currentTimeMillis() - start);
  }

  public static <T, V> FlatMapper<T, V> squelch(ThrowFlatMapper<T, V> f) {
    return (t, c) -> {
      try {
        f.flattenInto(t, c);
      } catch (Exception e) {
        squelchLog.log(Level.WARNING, String.valueOf(t), e);
      }
    };
  }

  interface ThrowFlatMapper<T, V> {
    void flattenInto(T t, Consumer<V> c) throws Exception;
  }
}
