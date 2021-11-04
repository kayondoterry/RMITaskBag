import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

// used for json serializing and deserializing
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import static java.lang.Thread.sleep;

public class Worker {

  
  // host machine of the TaskBag
  private String hostname = "localhost";
  // name of TaskBag on rmiregistry
  private String taskBagName = "TaskBag";

  final long WAIT_TIME = 4000L;

  public Worker(String taskBagHostname) {
    this.hostname = taskBagHostname;
  }

  public static void main(String[] args) {
    // get hostname command-line argument
    String host = "";
    try{
      host = args[0];
    }catch(Exception e){
      System.out.println("usage: Worker <hostname>");
      return;
    }

    Worker worker = new Worker(host);
    worker.run();
  }

  public void run() {
    try {
      // connect to rmiregistry of TaskBag host machine
      Registry registry = LocateRegistry.getRegistry(hostname);
      TaskBagRemote taskBagRemote = (TaskBagRemote) registry.lookup(taskBagName);

      System.out.println("Worker started\n");

      // start polling for the "Task" pair
      while (true) {
        // remove "Task" pair so other Workers don't get the same subtask
        String subTaskString = taskBagRemote.removePair("Task");
        if (subTaskString != null) {
          // decoded subtasks from JSON string
          List<List<Integer>> subTasks = getSubtasks(subTaskString);

          if(subTasks.size() < 1) {
            // tasks are finished, exit
            break;
          }

          // get first subtask in the list of subtasks
          List<Integer> range = subTasks.get(0);
          String resultId = getResultId(range);
          // encode the remaining subtasks to be placed back on the TaskBag
          subTaskString = generateSubTaskString(subTasks.subList(1, subTasks.size()));
          
          System.out.println("Received Task: " + resultId);

          // put back remaining subtasks on the TaskBag
          taskBagRemote.placePair("Task", subTaskString);

          // for storing prime numbers found
          List<Integer> result = new ArrayList<>();

          // go through the numbers in the range specified by subtask and find primes
          for(int n = range.get(0); n<= range.get(1); n++) {
            if(isPrime(n)) {
              result.add(n);
            }
          }

          // encode the result as JSON string
          String resultString = generateResultString(result);
          // put the result in the TaskBag
          taskBagRemote.placePair(resultId, resultString);

          System.out.println("Finished Task: " + resultId);

          if(subTasks.size() <= 1) {
            // this subtask was the last task, exit
            break;
          }

        } else {
          // if no "Task" pair is in the TaskBag, check again in WAIT_TIME seconds
          sleep(WAIT_TIME);
        }
      }

    } catch (Exception e) {
      System.err.println("Master exception:");
      e.printStackTrace();
    }

    System.out.println("Work done!");

  }

  // algorithm for calculating primes
  private boolean isPrime(int n) {
    if (n < 2)
      return false;
    if (n == 2)
      return true;
    if (n % 2 == 0)
      return false;
    for (long i = 3; i < Math.sqrt(n); i += 2) {
      if (n % i == 0)
        return false;
    }
    return true;
  }

  // decode subtasks from JSON string
  private List<List<Integer>> getSubtasks(String subTaskString) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.readValue(subTaskString, new TypeReference<List<List<Integer>>>() {
      });
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return null;
    }
  }

  // encode subtasks to JSON string
  private String generateSubTaskString(List<List<Integer>> subTaskList) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.writeValueAsString(subTaskList);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return null;
    }
  }
  // derive subtask/result id based on the range of numbers to be checked for primes
  private String getResultId(List<Integer> range) {
    return range.get(0) + "-" + range.get(1);
  }

  // encode results as JSON string
  private String generateResultString(List<Integer> result) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.writeValueAsString(result);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return null;
    }
  }

 
}
