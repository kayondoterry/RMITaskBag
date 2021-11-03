import java.rmi.Remote;
import java.rmi.RemoteException;

public interface TaskBagRemote extends Remote{
  void placePair(String key, String value) throws RemoteException;
  String takePair(String key) throws RemoteException;
  String removePair(String key) throws RemoteException;
}
