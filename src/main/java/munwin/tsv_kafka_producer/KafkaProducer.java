package munwin.tsv_kafka_producer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class KafkaProducer {

  /**
   * The default maximum number of messages to combine into
   * a single message payload. (used when loading
   * multiple messages from a tsv file)
   */
  private static final int DEFAULT_BATCH_SIZE = 1;

  /**
   * The default number of milliseconds to sleep between submitting
   * batched messages into the sorter. (Used when loading
   * multiple messages from a tsv file)
   */
  private static final int DEFAULT_BATCH_SLEEP = 10;

  /**
   * Path to the default configuration file for the program
   */
  public static final String DEFAULT_CONFIG_PATH =
      "conf/tsv_kafka_producer.conf";

  /**
   * Name of the queue to send messages to.
   */
  private final String destinationQueueName;

  /**
   * Kafka Producer variable
   */
  private Producer<String, String> kafkaProducer;

  /**
   * Instantiate KafkaProducer
   */
  public KafkaProducer(){
    this.destinationQueueName = null;
  }

  /**
   * Initialize the KafkaProducer with the path to a configuration file
   * containing configuration information.
   * @param configPath path to a config file containing kafka configuration.
   * @throws ConfigurationException if configPath isn't a path to an actual
   *                                configuration file.
   * @throws FileNotFoundException
   */
  public KafkaProducer(final String configPath)
      throws ConfigurationException, FileNotFoundException {
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.setDelimiterParsingDisabled(true);
    config.load(new FileReader(configPath));

    Properties props = new Properties();

    props.put("zk.connect", config.getString("kafka.zkConnect"));
    props.put("metadata.broker.list", config.getString("kafka.metadata.broker.list"));
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");

    ProducerConfig kafkaConfig = new ProducerConfig(props);
    this.kafkaProducer = new Producer<String, String>(kafkaConfig);
    this.destinationQueueName = config.getString("kafka.incomingQueue");
  }


  /**
   * Generate KafkaProducer with an existing Kafka Producer.
   * @param kafkaProducer an existing Kafka Producer.
   * @param destinationQueueName the name of the queue to publish messages to.
   */
  public KafkaProducer(
      final Producer<String, String> kafkaProducer,
      final String destinationQueueName) {
    this.kafkaProducer = kafkaProducer;
    this.destinationQueueName = destinationQueueName;
  }

  /**
   * Publish the contents of the messages at datafilePath to the kafka queue
   * @param datefilePath string path to a tsv file with the content to send
   *                     to the queue.
   * @param batchSize the maximum number of messages to include in the
   *                  payload of each message
   * @throws Exception
   */
  public void publishMessageFromTSVFile(final String datafilePath,
      final int batchSize, final int batchSleep) throws Exception {
    InputTsvFile inputFile = new InputTsvFile(datafilePath, batchSize);
    List<String> messages = inputFile.getMessages();

    for (String message : messages) {
      publishKafkaMessage(message);
      Thread.sleep(batchSleep);
    }
    System.out.println("Published " + messages.size() + " batched messages.");
  }

  /**
   * Publish the passed string as a message onto the configured kafka queue.
   * @param messageContent the contents of the message to create and publish.
   */
  public void publishKafkaMessage(final String messageContent) {
    KeyedMessage<String, String> data = new KeyedMessage<String, String>(destinationQueueName, messageContent);
    kafkaProducer.send(data);
  }


  /**
   * Run the KafkaProducer parsing configuration information from the
   * command-line arguments.
   *
   * @param args Command-line arguments for configuring the
   *             MessageFileGenerator.
   * @throws ConfigurationException if there's an issue reading the
   * configuration file indicated by the -c command-line option, or
   * DEFAULT_CONFIG_PATH
   * @throws IOException if there's an issue generating the MessageFiles
   */
  public static void main(final String[] args) throws ConfigurationException,
  IOException, Exception {

    Options options = new Options();

    OptionBuilder.withLongOpt("config");
    OptionBuilder.withDescription("path to the config file with the broker"
        + " and queue settings");
    OptionBuilder.hasArg();
    OptionBuilder.withArgName("PATH");
    options.addOption(OptionBuilder.create('c'));

    OptionBuilder.withLongOpt("datafile");
    OptionBuilder.withDescription("path to a file containing the messages to publish"
        + " in tsv format");
    OptionBuilder.hasArg();
    OptionBuilder.withArgName("DATAFILE");
    OptionBuilder.isRequired();
    options.addOption(OptionBuilder.create('d'));

    OptionBuilder.withLongOpt("batchsize");
    OptionBuilder.withDescription("maximum number of lines read from a"
        + " given data file to put in a single message. The default is: " + DEFAULT_BATCH_SIZE + ".\n"
        + " Must be combined with the -d|datafile option");
    OptionBuilder.hasArg();
    OptionBuilder.withArgName("BATCHSIZE");
    options.addOption(OptionBuilder.create('b'));

    OptionBuilder.withLongOpt("batchsleep");
    OptionBuilder.withDescription("number of milliseconds to delay between"
        + " submitting batched messages to the queue. The default is: " + DEFAULT_BATCH_SLEEP + ".\n"
        + " Must be combined with the -d|datafile option");
    OptionBuilder.hasArg();
    OptionBuilder.withArgName("BATCHSLEEP");
    options.addOption(OptionBuilder.create('s'));

    CommandLineParser parser = new PosixParser();
    CommandLine cli = null;

    try {
      cli = parser.parse(options, args);
    } catch (ParseException e) {
      System.err.println("Parsing the command-line options failed. Reason: "
          + e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(" ", options);
      System.exit(1);
    }

    KafkaProducer kafkaProducer = null;
    if ((cli != null) && cli.hasOption('c')) {
      String configPath = cli.getOptionValue('c');
      System.out.println("Instantiating message file generator with "
          + configPath);
      kafkaProducer = new KafkaProducer(configPath);
    }
    if (kafkaProducer == null) {
      kafkaProducer = new KafkaProducer(DEFAULT_CONFIG_PATH);
    }

    if ((cli != null) && cli.hasOption('d')) {
      String datafilePath = cli.getOptionValue('d');
      String batchSizeOveride = cli.getOptionValue('b');
      int batchSize = DEFAULT_BATCH_SIZE;
      if (batchSizeOveride != null) {
        try {
          batchSize = Integer.parseInt(batchSizeOveride);
        } catch (NumberFormatException nfe) {
          System.out.print("Message limit given with '-b' parameter is not a "
              + "valid number. Using default size: " + DEFAULT_BATCH_SIZE);
          batchSize = DEFAULT_BATCH_SIZE;
        }
      }

      String batchSleepOveride = cli.getOptionValue('s');
      int batchSleep = DEFAULT_BATCH_SLEEP;
      if (batchSleepOveride != null) {
        try {
          batchSleep = Integer.parseInt(batchSleepOveride);
        } catch (NumberFormatException nfe) {
          System.out.print(
              "Batch sleep duration given with '-s' parameter is not a valid " +
              "number. Using default delay: " + DEFAULT_BATCH_SLEEP + " milliseconds"
          );
          batchSleep = DEFAULT_BATCH_SLEEP;
        }
      }
      System.out.print("Publishing message contents from:" + datafilePath);
      kafkaProducer.publishMessageFromTSVFile(
          datafilePath,
          batchSize,
          batchSleep
      );
      System.out.println("done.");
    } 

    System.exit(0);
  }
}

