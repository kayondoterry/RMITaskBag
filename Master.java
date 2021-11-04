import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
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
  // holds results of computations by workers
  private Map<String, List<Integer>> results;
  // prime numbers between 1 and N
  // subTaskSize is the range of numbers for each subtask processed by a worker
  private int N, subTaskSize;

  public Master(String taskBagHostname, int N, int subTaskSize) {
    this.results = new TreeMap<>();
    this.N = N;
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
    System.out.println("Prime numbers between 1 and N.");
    System.out.println("Enter N: ");
    int N= in.nextInt();
    System.out.println("Enter range size for each sub task: ");
    int subTaskSize= in.nextInt();


    Master master = new Master(host, N, subTaskSize);
    master.run();

  }

  public void run() {
    // break Task into subtasks
    List<List<Integer>> subTasks = generateSubTasks(N, subTaskSize);
    // subTasks encoded as JSON String
    // Strings can be sent to remote objects because they implement the Serializable interface
    String subTaskString = generateSubTaskString(subTasks);

    try {
      // connect to rmiregistry of TaskBag host machine
      Registry registry = LocateRegistry.getRegistry(hostname);
      TaskBagRemote taskBagRemote = (TaskBagRemote) registry.lookup(taskBagName);

      // add sub tasks to the remote TaskBag
      taskBagRemote.placePair("Task", subTaskString);

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
    
    System.out.println("The prime numbers between 1 and " + N + " are:\n");
    // print the results
    for (Map.Entry<String, List<Integer>> entry : results.entrySet()) {
      List<Integer> primes = entry.getValue();
      for (Integer prime : primes) {
        System.out.print(prime + ", ");
      }
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
