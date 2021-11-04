import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

public class TaskBag implements TaskBagRemote {
  // used to store pairs
  private final Map<String, String> taskBag;
  // name of TaskBag in rmiregistry
  private String taskBagName = "TaskBag";

  public TaskBag() {
    taskBag = new HashMap<>();
  }

  public static void main(String[] args) {
    TaskBag taskBag = new TaskBag();
    taskBag.run();
  }

  public void run() {
    try {
      // create TaskBag stub to be remotely invoked by Master and Workers
      TaskBagRemote stub = (TaskBagRemote) UnicastRemoteObject.exportObject(this, 0);
      Registry registry = LocateRegistry.getRegistry();
      registry.rebind(taskBagName, stub);
      System.out.println("Task Bag Active");
    } catch (Exception e) {
      System.err.println("Task Bag exception:");
      e.printStackTrace();
    }
  }

  // methods synchronized to ensure that only one Worker gets a subtask at a time

  @Override
  synchronized public void placePair(String key, String value) {
    taskBag.put(key, value);
    System.out.println("placePair() called: [key: "+key+",value: "+value+"]\n");
  }

  @Override
  synchronized public String takePair(String key) {
    System.out.println("takePair() called: [key: "+key+"]\n");
    return taskBag.get(key);
  }

  @Override
  synchronized public String removePair(String key) {
    System.out.println("removePair() called: [key: "+key+"]\n");
    return taskBag.remove(key);
  }

}
