package idv.kyle;

import idv.kyle.hdfs.ConvertTextToSequence;
import idv.kyle.hdfs.SequenceFileReadDemo;
import idv.kyle.hdfs.SequenceFileWriter;

public class HadoopTool {

  public static void main(String[] args) throws Exception {
    HadoopTool tool = new HadoopTool();
    tool.runTool(args);
  }

  public void runTool(String[] args) throws Exception {
    if (args.length < 1) {
      this.printUsageInfo();
    } else {
      if ("convertTextToSeq".equals(args[0])) {
        if (args.length != 3) {
          throw new IllegalArgumentException("Incorrect number of arguments");
        } else {
          ConvertTextToSequence tool = new ConvertTextToSequence();
          tool.mainRun(args[1], args[2]);
        }
      } else if ("readSeqFile".equals(args[0])) {
        if (args.length != 2) {
          throw new IllegalArgumentException("Incorrect number of arguments");
        } else {
          SequenceFileReadDemo tool = new SequenceFileReadDemo();
          tool.mainRun(args[1]);
        }
      } else if ("writeSeqFileToLocal".equals(args[0])) {
        if (args.length != 2) {
          throw new IllegalArgumentException("Incorrect number of arguments");
        } else {
          SequenceFileWriter tool = new SequenceFileWriter();
          tool.mainRun(args[1]);
        }
      }
    }
  }

  private void printUsageInfo() {
    System.out.println("Usage: jarName <ToolName> [args...]");
    System.out.println("ToolName Choice: convertTextToSeq");
    System.out.println("");
    System.out.println("convertTextToSeq Usage: jarName convertTextToSeq inputPath outputPath");
    System.out.println("");
    System.out.println("seqFileReadDemo Usage: jarName seqFileReadDemo fileUri");
  }
}
