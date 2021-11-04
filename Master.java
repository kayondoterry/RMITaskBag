import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


public class Master {

  private String hostname = "localhost";
  private String taskBagName = "TaskBag";
  private Map<String, List<Integer>> results;
  private int N, subTaskSize;

  public Master(String taskBagHostname, int N, int subTaskSize) {
    this.results = new TreeMap<>();
    this.N = N;
    this.subTaskSize = subTaskSize;
    this.hostname = taskBagHostname;
  }

  public static void main(String[] args) {

    String host = "";
    try{
      host = args[0];
    }catch(Exception e){
      System.out.println("usage: Master <hostname>");
      return;
    }

    Scanner in = new Scanner(System.in);

    System.out.println("Prime numbers between 1 and N.");
    System.out.println("Enter N: ");
    int N= in.nextInt();
    System.out.println("Enter range size for each sub task: ");
    int subTaskSize= in.nextInt();


    Master master = new Master(host, N, subTaskSize);
    master.run();

  }

  public void run() {

    List<List<Integer>> subTasks = generateSubTasks(N, subTaskSize);
    String subTaskString = generateSubTaskString(subTasks);

    try {
      Registry registry = LocateRegistry.getRegistry(hostname); // TODO getRegistry()
      TaskBagRemote taskBagRemote = (TaskBagRemote) registry.lookup(taskBagName);

      taskBagRemote.placePair("Task", subTaskString);

      int totalSubTasks = subTasks.size();

      System.out.println("Master started\nwaiting for workers.......\n");

      while (subTasks.size() > 0) {
        for (List<Integer> range : new ArrayList<>(subTasks)) {
          String resultId = getResultId(range);
          String resultString = taskBagRemote.removePair(resultId);

          if (resultString != null) {
            subTasks.remove(range);
            List<Integer> result = getResult(resultString);
            results.put(resultId, result);
            System.out.println("Got result " + results.size() + "/" + totalSubTasks + ": " + resultId);
          }
        }
      }

    } catch (Exception e) {
      System.err.println("Master exception:");
      e.printStackTrace();
    }

    System.out.println("The prime numbers between 1 and " + N + " are:\n");

    for (Map.Entry<String, List<Integer>> entry : results.entrySet()) {
      List<Integer> primes = entry.getValue();
      for (Integer prime : primes) {
        System.out.print(prime + ", ");
      }
    }

  }

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

  private String generateSubTaskString(List<List<Integer>> subTasks) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.writeValueAsString(subTasks);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return null;
    }

  }

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

  private String getResultId(List<Integer> range) {
    return range.get(0) + "-" + range.get(1);
  }
}
