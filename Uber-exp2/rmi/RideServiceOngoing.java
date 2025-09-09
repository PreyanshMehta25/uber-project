import java.rmi.*;
import java.rmi.server.*;
import java.rmi.registry.*;

interface RideService extends Remote {
    double calculateFare(double distance, double rate) throws RemoteException;
    String getTripStatus(String rider) throws RemoteException;
    String getFullTripDetails(String rider, double distance, double rate) throws RemoteException;
}

public class RideServiceOngoing extends UnicastRemoteObject implements RideService {
    
    protected RideServiceOngoing() throws RemoteException {
        super();
    }

    @Override
    public double calculateFare(double distance, double rate) throws RemoteException {
        return distance * rate;
    }

    @Override
    public String getTripStatus(String rider) throws RemoteException {
        return "Trip for " + rider + " is ongoing...";
    }

    @Override
    public String getFullTripDetails(String rider, double distance, double rate) throws RemoteException {
        double fare = calculateFare(distance, rate);

        System.out.println("Rider: " + rider + " | Distance: " + distance + " km | Rate: " + rate + " per km | Fare: " + fare);

        return "Trip for " + rider + " is ongoing... | Fare: " + fare;
    }

    public static void main(String[] args) {
        try {
            RideServiceOngoing obj = new RideServiceOngoing();

            LocateRegistry.createRegistry(1099);

            Naming.rebind("RideService", obj);

            System.out.println("Uber RMI Server ready...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
