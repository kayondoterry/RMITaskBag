import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import static java.lang.Thread.sleep;

public class Worker {

  private String hostname = "localhost";
  private String taskBagName = "TaskBag";

  public static void main(String[] args) {
    Worker worker = new Worker();
    worker.run();
  }

  public void run() {
    try {
      Registry registry = LocateRegistry.getRegistry(hostname); // TODO getRegistry()
      TaskBagRemote taskBagRemote = (TaskBagRemote) registry.lookup(taskBagName);

      System.out.println("Worker started\n");

      while (true) {
        String subTaskString = taskBagRemote.removePair("Task");
        if (subTaskString != null) {
          List<List<Integer>> subTasks = getSubtasks(subTaskString);

          if(subTasks.size() < 1) {
            break;
          }

          List<Integer> range = subTasks.get(0);
          String resultId = getResultId(range);
          subTaskString = generateSubTaskString(subTasks.subList(1, subTasks.size()));
          
          taskBagRemote.placePair("Task", subTaskString);

          List<Integer> result = new ArrayList<>();

          for(int n = range.get(0); n<= range.get(1); n++) {
            if(isPrime(n)) {
              result.add(n);
            }
          }

          String resultString = generateResultString(result);
          taskBagRemote.placePair(resultId, resultString);

          if(subTasks.size() <= 1) {
            break;
          }

        } else {
          sleep(5000);
        }
      }

    } catch (Exception e) {
      System.err.println("Master exception:");
      e.printStackTrace();
    }

    System.out.println("Work done!");

  }

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

  private String generateSubTaskString(List<List<Integer>> subTaskList) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.writeValueAsString(subTaskList);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return null;
    }
  }

  private String getResultId(List<Integer> range) {
    return range.get(0) + "-" + range.get(1);
  }

  private String generateResultString(List<Integer> result) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.writeValueAsString(result);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return null;
    }
  }

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
}
