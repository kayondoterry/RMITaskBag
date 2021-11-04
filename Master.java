import java.io.PrintWriter;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;


// used for json serializing and deserializing
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


public class Master {
  // host machine of the TaskBag
  private String hostname = "localhost";

  // name of TaskBag on rmiregistry
  private String taskBagName = "TaskBag";

  // results from Workers stored here
  private TreeMap<String, List<Integer>> results;

  private final String NEXT_TASK = "NextTask";

  // prime numbers between 1 and N
  // subTaskSize is the range of numbers for each subtask processed by a worker
  private int MAX, subTaskSize;
  public Master(String taskBagHostname, int MAX, int subTaskSize) {
    this.results = new TreeMap<>(new Comparator<String>() {
      @Override
      public int compare(String s1, String s2) {
          Integer num1 = Integer.parseInt(s1.split("-")[0]);
          Integer num2 = Integer.parseInt(s2.split("-")[0]);
          return num1.compareTo(num2);
      }
    });
    this.MAX = MAX;
    this.subTaskSize = subTaskSize;
    this.hostname = taskBagHostname;
  }
  public static void main(String[] args) {
    // get hostname command-line argument
    String host = "";
    try{
      host = args[0];
    }catch(Exception e){
      System.out.println("usage: Master <hostname>");
      return;
    }
    Scanner in = new Scanner(System.in);
    // get required information from users
    while(true) {
      System.out.println("Prime numbers between 1 and MAX.");
      System.out.println("Enter MAX: (-1 to quit)");
      int MAX = in.nextInt();
      if(MAX == -1) {
        break;
      }
      if(MAX < 1) {
        System.out.println("MAX must be greater than 0");
        break;
      }
      System.out.println("Enter range size for each sub task: ");
      int subTaskSize= in.nextInt();
      Master master = new Master(host, MAX, subTaskSize);
      master.run();
    }
  }
  public void run() {
    // break Task into subtasks
    List<List<Integer>> subTasks = generateSubTasks(MAX, subTaskSize);
    // subTasks encoded as JSON String
    // Strings can be sent to remote objects because they implement the Serializable interface
    String subTaskString = generateSubTaskString(subTasks);
    try {
      // connect to rmiregistry of TaskBag host machine
      Registry registry = LocateRegistry.getRegistry(hostname);
      TaskBagRemote taskBagRemote = (TaskBagRemote) registry.lookup(taskBagName);
      // add sub tasks to the remote TaskBag
      taskBagRemote.placePair(NEXT_TASK, subTaskString);
      // used for progress updates
      int totalSubTasks = subTasks.size();
      System.out.println("Master started\nwaiting for workers.......\n");
      // keep checking TaskBag for completed work by Workers
      while (subTasks.size() > 0) {
        // iterate through the subtasks and check it their results are in the TaskBag
        for (List<Integer> range : new ArrayList<>(subTasks)) {
          String resultId = getResultId(range);
          String resultString = taskBagRemote.removePair(resultId);
          if (resultString != null) {// result retrieved
            subTasks.remove(range); // remove subtask from list of those to be checked
            List<Integer> result = getResult(resultString); // decode result into List of primes
            results.put(resultId, result); // store the results
            // progress update
            System.out.println("Got result " + results.size() + "/" + totalSubTasks + ": " + resultId);
          }
        }
      }
    } catch (Exception e) {
      System.err.println("Master exception:");
      e.printStackTrace();
    }
    
    System.out.println("The prime numbers between 1 and " + MAX + " are:\n");

    // print the results on stdout and a file
    PrintWriter writer = null;
    try {
      writer = new PrintWriter("primes between 1 and " + MAX, "UTF-8");
    } catch (Exception e) {
      System.out.println("Error: Failed to create results file");
      return;
    }

    for (Map.Entry<String, List<Integer>> entry : results.entrySet()) {
      List<Integer> primes = entry.getValue();
      for (Integer prime : primes) {
        if(writer!=null) {
          writer.println(prime);
        }
      }
    }

    if(writer != null) {
      writer.close();
      System.out.println("Task results written to file: primes between 1 and " + MAX);
    }

  }
  // breaks a Task into subtasks
  private List<List<Integer>> generateSubTasks(int N, int subTaskSize) {
    int totalSubTasks = N / subTaskSize;
    int remainder = N % subTaskSize;
    List<List<Integer>> subTaskList = new ArrayList<>();
    for (int i = 1; i <= totalSubTasks; i += 1) {
      int start = subTaskSize * (i - 1) + 1;
      int end = subTaskSize * i;
      subTaskList.add(Arrays.asList(new Integer[] { start, end }));
    }
    if (remainder > 0) {
      int start = totalSubTasks * subTaskSize + 1;
      int end = N;
      subTaskList.add(Arrays.asList(new Integer[] { start, end }));
    }
    return subTaskList;
  }
  // encodes subtasks as JSON string
  private String generateSubTaskString(List<List<Integer>> subTasks) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.writeValueAsString(subTasks);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return null;
    }
  }
  // decode list of primes from JSON string
  private List<Integer> getResult(String resultString) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.readValue(resultString, new TypeReference<List<Integer>>() {
      });
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return null;
    }
  }
  // derive subtask/result id based on the range of numbers to be checked for primes
  private String getResultId(List<Integer> range) {
    return range.get(0) + "-" + range.get(1);
  }
}
