import java.rmi.Remote;
import java.rmi.RemoteException;

public interface TaskBagRemote extends Remote{
  
  // adds a Pair() to the task bag
  void placePair(String key, String value) throws RemoteException;

  // gets a Pair() with the key, without removing it from the task bag
  String takePair(String key) throws RemoteException;

  // gets a pair with the key and removes the pair from the task bag
  String removePair(String key) throws RemoteException;
}
