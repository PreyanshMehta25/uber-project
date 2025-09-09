import java.rmi.*;

public interface RideService extends Remote {
    double calculateFare(double distance, double rate) throws RemoteException;
    String getTripStatus(String rider) throws RemoteException;
    String getFullTripDetails(String rider, double distance, double rate) throws RemoteException;
}
