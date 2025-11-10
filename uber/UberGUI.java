import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.net.*;

public class UberGUI extends JFrame {
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private int localTime = 0;
    
    private CardLayout cardLayout;
    private JPanel mainPanel;
    private JTextArea logArea;
    
    private String currentUser = null;
    private String currentRideId = null;
    private double currentFare = 0.0;
    private boolean driversRegistered = false;
    
    // Real trip data
    private double tripDistance = 0.0;
    private double tripRate = 0.0;
    private String pickupLocation = "";
    private String destinationLocation = "";
    private String assignedDriver = "";
    private String assignedDriverVehicle = "";
    private String assignedDriverPhone = "";
    private String assignedDriverRating = "4.8";
    
    public UberGUI() {
        setTitle("Uber - Complete Ride Experience");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(800, 600);
        setLocationRelativeTo(null);
        
        initUI();
        connectToServer();
    }
    
    private void initUI() {
        cardLayout = new CardLayout();
        mainPanel = new JPanel(cardLayout);
        
        mainPanel.add(createWelcome(), "WELCOME");
        mainPanel.add(createRegister(), "REGISTER");
        mainPanel.add(createBooking(), "BOOKING");
        mainPanel.add(createTrip(), "TRIP");
        mainPanel.add(createPayment(), "PAYMENT");
        
        setLayout(new BorderLayout());
        add(mainPanel, BorderLayout.CENTER);
        
        logArea = new JTextArea(6, 50);
        logArea.setEditable(false);
        logArea.setBackground(Color.BLACK);
        logArea.setForeground(Color.GREEN);
        logArea.setFont(new Font("Consolas", Font.PLAIN, 10));
        JScrollPane logScroll = new JScrollPane(logArea);
        logScroll.setBorder(BorderFactory.createTitledBorder("System Log"));
        add(logScroll, BorderLayout.SOUTH);
        
        cardLayout.show(mainPanel, "WELCOME");
    }
    
    private JPanel createWelcome() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBackground(Color.BLACK);
        
        JLabel logo = new JLabel("UBER", SwingConstants.CENTER);
        logo.setFont(new Font("Arial", Font.BOLD, 48));
        logo.setForeground(Color.WHITE);
        
        JLabel tagline = new JLabel("Move with Uber", SwingConstants.CENTER);
        tagline.setFont(new Font("Arial", Font.PLAIN, 18));
        tagline.setForeground(Color.LIGHT_GRAY);
        
        JPanel headerPanel = new JPanel(new BorderLayout());
        headerPanel.setBackground(Color.BLACK);
        headerPanel.add(logo, BorderLayout.CENTER);
        headerPanel.add(tagline, BorderLayout.SOUTH);
        
        JPanel buttonPanel = new JPanel(new GridLayout(2, 1, 0, 20));
        buttonPanel.setBackground(Color.BLACK);
        buttonPanel.setBorder(BorderFactory.createEmptyBorder(50, 100, 100, 100));
        
        JButton riderBtn = new JButton("I need a ride");
        JButton driverBtn = new JButton("I want to drive");
        
        riderBtn.setBackground(Color.WHITE);
        riderBtn.setForeground(Color.BLACK);
        riderBtn.setFont(new Font("Arial", Font.BOLD, 16));
        
        driverBtn.setBackground(Color.LIGHT_GRAY);
        driverBtn.setForeground(Color.BLACK);
        driverBtn.setFont(new Font("Arial", Font.BOLD, 16));
        
        riderBtn.addActionListener(e -> {
            currentUser = "rider";
            showRegistrationForm();
        });
        
        driverBtn.addActionListener(e -> {
            currentUser = "driver";
            showRegistrationForm();
        });
        
        buttonPanel.add(riderBtn);
        buttonPanel.add(driverBtn);
        
        panel.add(headerPanel, BorderLayout.NORTH);
        panel.add(buttonPanel, BorderLayout.CENTER);
        
        return panel;
    }
    
    private JPanel createRegister() {
        return new JPanel(); // Placeholder - will be replaced dynamically
    }
    
    private void showRegistrationForm() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBackground(Color.WHITE);
        
        JButton backBtn = new JButton("Back");
        backBtn.addActionListener(e -> cardLayout.show(mainPanel, "WELCOME"));
        
        String titleText = "driver".equals(currentUser) ? "Driver Registration" : "Rider Registration";
        JLabel title = new JLabel(titleText, SwingConstants.CENTER);
        title.setFont(new Font("Arial", Font.BOLD, 24));
        
        JPanel form = new JPanel(new GridBagLayout());
        form.setBackground(Color.WHITE);
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(10, 10, 10, 10);
        
        JTextField nameField = new JTextField(20);
        JTextField phoneField = new JTextField(20);
        JTextField locationField = new JTextField(20);
        
        gbc.gridx = 0; gbc.gridy = 0;
        form.add(new JLabel("Name:"), gbc);
        gbc.gridx = 1;
        form.add(nameField, gbc);
        
        gbc.gridx = 0; gbc.gridy = 1;
        form.add(new JLabel("Phone:"), gbc);
        gbc.gridx = 1;
        form.add(phoneField, gbc);
        
        // Add location field for drivers
        if ("driver".equals(currentUser)) {
            gbc.gridx = 0; gbc.gridy = 2;
            form.add(new JLabel("Location:"), gbc);
            gbc.gridx = 1;
            form.add(locationField, gbc);
        }
        
        JButton continueBtn = new JButton("Continue");
        continueBtn.setBackground(Color.BLACK);
        continueBtn.setForeground(Color.WHITE);
        
        continueBtn.addActionListener(e -> {
            String name = nameField.getText().trim();
            String phone = phoneField.getText().trim();
            
            if (name.isEmpty() || phone.isEmpty()) {
                JOptionPane.showMessageDialog(this, "Please fill all fields");
                return;
            }
            
            if ("driver".equals(currentUser)) {
                String location = locationField.getText().trim();
                if (location.isEmpty()) {
                    JOptionPane.showMessageDialog(this, "Please enter location");
                    return;
                }
                sendRequest("REGISTER_DRIVER;" + name + ";" + location);
                log("Driver registered: " + name);
                driversRegistered = true; // Mark that we have drivers
                JOptionPane.showMessageDialog(this, "Driver registered successfully!\nYou can now register more drivers or book rides.");
                // Reset currentUser and go back to welcome
                currentUser = null;
                cardLayout.show(mainPanel, "WELCOME");
            } else {
                currentUser = name;
                log("Rider registered: " + name);
                cardLayout.show(mainPanel, "BOOKING");
            }
        });
        
        // Position button based on whether location field is shown
        gbc.gridx = 0; 
        gbc.gridy = "driver".equals(currentUser) ? 3 : 2;
        gbc.gridwidth = 2;
        form.add(continueBtn, gbc);
        
        JPanel topPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        topPanel.setBackground(Color.WHITE);
        topPanel.add(backBtn);
        
        panel.add(topPanel, BorderLayout.NORTH);
        panel.add(title, BorderLayout.CENTER);
        panel.add(form, BorderLayout.SOUTH);
        
        // Replace the register panel and show it
        mainPanel.remove(mainPanel.getComponent(1)); // Remove old register panel
        mainPanel.add(panel, "REGISTER", 1); // Add new panel at index 1
        cardLayout.show(mainPanel, "REGISTER");
    }
    
    private JPanel createBooking() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBackground(Color.WHITE);
        
        JPanel header = new JPanel(new BorderLayout());
        header.setBackground(Color.BLACK);
        header.setBorder(BorderFactory.createEmptyBorder(15, 20, 15, 20));
        
        JLabel welcome = new JLabel("Book Your Ride" + (currentUser != null && !currentUser.equals("rider") ? " - " + currentUser : ""));
        welcome.setForeground(Color.WHITE);
        welcome.setFont(new Font("Arial", Font.BOLD, 18));
        
        JButton logoutBtn = new JButton("Logout");
        logoutBtn.addActionListener(e -> {
            currentUser = null;
            cardLayout.show(mainPanel, "WELCOME");
        });
        
        JButton hdfsBtn = new JButton("HDFS Status");
        hdfsBtn.addActionListener(e -> showHDFSStatus());
        
        JButton faultBtn = new JButton("Fault Tolerance");
        faultBtn.addActionListener(e -> showFaultToleranceMenu());
        
        JPanel headerButtons = new JPanel(new FlowLayout());
        headerButtons.setBackground(Color.BLACK);
        headerButtons.add(hdfsBtn);
        headerButtons.add(faultBtn);
        headerButtons.add(logoutBtn);
        
        header.add(welcome, BorderLayout.WEST);
        header.add(headerButtons, BorderLayout.EAST);
        
        JPanel bookingPanel = new JPanel(new GridBagLayout());
        bookingPanel.setBackground(Color.WHITE);
        bookingPanel.setBorder(BorderFactory.createEmptyBorder(30, 40, 30, 40));
        
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(15, 10, 15, 10);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        
        JTextField pickupField = new JTextField(30);
        JTextField destinationField = new JTextField(30);
        
        gbc.gridx = 0; gbc.gridy = 0;
        bookingPanel.add(new JLabel("Pickup Location:"), gbc);
        gbc.gridy = 1;
        bookingPanel.add(pickupField, gbc);
        
        gbc.gridy = 2;
        bookingPanel.add(new JLabel("Destination:"), gbc);
        gbc.gridy = 3;
        bookingPanel.add(destinationField, gbc);
        
        JButton estimateBtn = new JButton("Get Fare Estimate");
        estimateBtn.setBackground(new Color(230, 230, 230));
        
        JLabel fareLabel = new JLabel("", SwingConstants.CENTER);
        fareLabel.setFont(new Font("Arial", Font.BOLD, 16));
        fareLabel.setForeground(new Color(0, 150, 0));
        
        estimateBtn.addActionListener(e -> {
            if (!pickupField.getText().trim().isEmpty() && !destinationField.getText().trim().isEmpty()) {
                // Ask user for distance and rate
                String distanceStr = JOptionPane.showInputDialog(this, "Enter trip distance (km):", "Distance", JOptionPane.QUESTION_MESSAGE);
                if (distanceStr == null || distanceStr.trim().isEmpty()) return;
                
                String rateStr = JOptionPane.showInputDialog(this, "Enter rate per km ($):", "Rate per KM", JOptionPane.QUESTION_MESSAGE);
                if (rateStr == null || rateStr.trim().isEmpty()) return;
                
                try {
                    double distance = Double.parseDouble(distanceStr.trim());
                    double rate = Double.parseDouble(rateStr.trim());
                    
                    // Store real trip data
                    tripDistance = distance;
                    tripRate = rate;
                    
                    sendRequest("CALCULATE_FARE;" + String.format("%.1f", distance) + ";" + String.format("%.2f", rate));
                    
                    // Calculate fare with surge
                    double surge = 1.0 + (Math.random() * 0.5);
                    currentFare = distance * rate * surge;
                    
                    fareLabel.setText("Estimated Fare: $" + String.format("%.2f", currentFare) + 
                                    " (Distance: " + distance + "km, Rate: $" + rate + "/km, Surge: " + String.format("%.1fx", surge) + ")");
                    log("Fare estimated: $" + String.format("%.2f", currentFare) + " for " + distance + "km at $" + rate + "/km");
                } catch (NumberFormatException ex) {
                    JOptionPane.showMessageDialog(this, "Please enter valid numbers for distance and rate");
                }
            } else {
                JOptionPane.showMessageDialog(this, "Please enter pickup and destination");
            }
        });
        
        JButton bookBtn = new JButton("Request Ride");
        bookBtn.setBackground(Color.BLACK);
        bookBtn.setForeground(Color.WHITE);
        bookBtn.setFont(new Font("Arial", Font.BOLD, 16));
        
        bookBtn.addActionListener(e -> {
            String pickup = pickupField.getText().trim();
            String destination = destinationField.getText().trim();
            
            if (pickup.isEmpty() || destination.isEmpty()) {
                JOptionPane.showMessageDialog(this, "Please fill pickup and destination");
                return;
            }
            
            if (currentFare == 0) {
                JOptionPane.showMessageDialog(this, "Please get fare estimate first");
                return;
            }
            
            // Check if drivers are registered
            if (!driversRegistered) {
                JOptionPane.showMessageDialog(this, "No drivers available!\nPlease register drivers first by choosing 'I want to drive' from the main menu.");
                return;
            }
            
            // Ask for rider name if not set
            if (currentUser == null || currentUser.equals("rider")) {
                String riderName = JOptionPane.showInputDialog(this, "Enter your name:", "Rider Name", JOptionPane.QUESTION_MESSAGE);
                if (riderName == null || riderName.trim().isEmpty()) {
                    JOptionPane.showMessageDialog(this, "Please enter your name");
                    return;
                }
                currentUser = riderName.trim();
            }
            
            // Store pickup and destination
            pickupLocation = pickup;
            destinationLocation = destination;
            
            sendRequest("REQUEST_RIDE;" + currentUser + ";" + pickup + ";" + destination);
            currentRideId = "RIDE_" + currentUser + "_" + System.currentTimeMillis();
            
            log("Ride requested by " + currentUser + " from " + pickup + " to " + destination);
            
            // Show finding driver message
            JOptionPane.showMessageDialog(this, "Finding available driver...");
            
            Timer timer = new Timer(3000, event -> {
                sendRequest("ASSIGN_DRIVER;" + currentRideId);
                log("Attempting to assign driver to ride: " + currentRideId);
                
                // Simulate driver assignment check
                Timer checkTimer = new Timer(1000, checkEvent -> {
                    log("Driver found and assigned!");
                    showTripScreen();
                });
                checkTimer.setRepeats(false);
                checkTimer.start();
            });
            timer.setRepeats(false);
            timer.start();
        });
        
        gbc.gridy = 4;
        bookingPanel.add(estimateBtn, gbc);
        
        gbc.gridy = 5;
        bookingPanel.add(fareLabel, gbc);
        
        gbc.gridy = 6;
        gbc.insets = new Insets(20, 10, 20, 10);
        bookingPanel.add(bookBtn, gbc);
        
        panel.add(header, BorderLayout.NORTH);
        panel.add(bookingPanel, BorderLayout.CENTER);
        
        return panel;
    }    
   
 private JPanel createTrip() {
        return new JPanel(); // Placeholder - will be replaced dynamically
    }
    
    private void showTripScreen() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBackground(Color.WHITE);
        
        JPanel header = new JPanel(new BorderLayout());
        header.setBackground(new Color(0, 150, 0));
        header.setBorder(BorderFactory.createEmptyBorder(20, 25, 20, 25));
        
        JLabel tripStatus = new JLabel("Your driver is on the way!");
        tripStatus.setFont(new Font("Arial", Font.BOLD, 20));
        tripStatus.setForeground(Color.WHITE);
        
        JButton cancelBtn = new JButton("Cancel Ride");
        cancelBtn.setBackground(Color.RED);
        cancelBtn.setForeground(Color.WHITE);
        cancelBtn.addActionListener(e -> {
            int result = JOptionPane.showConfirmDialog(this, "Cancel this ride?", "Cancel Ride", JOptionPane.YES_NO_OPTION);
            if (result == JOptionPane.YES_OPTION) {
                log("Ride cancelled");
                cardLayout.show(mainPanel, "BOOKING");
            }
        });
        
        header.add(tripStatus, BorderLayout.WEST);
        header.add(cancelBtn, BorderLayout.EAST);
        
        JPanel detailsPanel = new JPanel(new GridBagLayout());
        detailsPanel.setBackground(Color.WHITE);
        detailsPanel.setBorder(BorderFactory.createEmptyBorder(30, 40, 30, 40));
        
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(15, 10, 15, 10);
        gbc.anchor = GridBagConstraints.WEST;
        
        // Display real driver information
        gbc.gridx = 0; gbc.gridy = 0;
        JLabel driverLabel = new JLabel("Driver: " + (assignedDriver.isEmpty() ? "Assigning..." : assignedDriver));
        driverLabel.setFont(new Font("Arial", Font.BOLD, 16));
        detailsPanel.add(driverLabel, gbc);
        
        gbc.gridy = 1;
        JLabel vehicleLabel = new JLabel("Vehicle: " + (assignedDriverVehicle.isEmpty() ? "..." : assignedDriverVehicle));
        vehicleLabel.setFont(new Font("Arial", Font.PLAIN, 14));
        detailsPanel.add(vehicleLabel, gbc);
        
        gbc.gridy = 2;
        JLabel phoneLabel = new JLabel("Phone: " + (assignedDriverPhone.isEmpty() ? "..." : assignedDriverPhone));
        phoneLabel.setFont(new Font("Arial", Font.PLAIN, 14));
        detailsPanel.add(phoneLabel, gbc);
        
        gbc.gridy = 3;
        JLabel ratingLabel = new JLabel("Rating: " + assignedDriverRating + "/5.0 *");
        ratingLabel.setFont(new Font("Arial", Font.PLAIN, 14));
        ratingLabel.setForeground(new Color(255, 165, 0));
        detailsPanel.add(ratingLabel, gbc);
        
        gbc.gridy = 4;
        gbc.insets = new Insets(25, 10, 10, 10);
        JLabel tripLabel = new JLabel("Trip Details:");
        tripLabel.setFont(new Font("Arial", Font.BOLD, 14));
        detailsPanel.add(tripLabel, gbc);
        
        gbc.gridy = 5;
        gbc.insets = new Insets(5, 10, 5, 10);
        detailsPanel.add(new JLabel("From: " + pickupLocation), gbc);
        
        gbc.gridy = 6;
        detailsPanel.add(new JLabel("To: " + destinationLocation), gbc);
        
        gbc.gridy = 7;
        gbc.insets = new Insets(20, 10, 15, 10);
        JLabel fareLabel = new JLabel("Estimated Fare: $" + String.format("%.2f", currentFare));
        fareLabel.setFont(new Font("Arial", Font.BOLD, 16));
        fareLabel.setForeground(new Color(0, 150, 0));
        detailsPanel.add(fareLabel, gbc);
        
        JButton completeBtn = new JButton("Complete Trip");
        completeBtn.setBackground(new Color(0, 150, 0));
        completeBtn.setForeground(Color.WHITE);
        completeBtn.setFont(new Font("Arial", Font.BOLD, 14));
        completeBtn.addActionListener(e -> {
            log("Trip completed");
            showPaymentScreen();
        });
        
        gbc.gridy = 8;
        gbc.insets = new Insets(40, 10, 15, 10);
        detailsPanel.add(completeBtn, gbc);
        
        panel.add(header, BorderLayout.NORTH);
        panel.add(detailsPanel, BorderLayout.CENTER);
        
        // Replace the trip panel and show it
        mainPanel.remove(mainPanel.getComponent(3)); // Remove old trip panel
        mainPanel.add(panel, "TRIP", 3); // Add new panel at index 3
        cardLayout.show(mainPanel, "TRIP");
    } 
   
    private JPanel createPayment() {
        return new JPanel(); // Placeholder - will be replaced dynamically
    }
    
    private void showPaymentScreen() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBackground(Color.WHITE);
        
        JPanel header = new JPanel(new FlowLayout(FlowLayout.CENTER));
        header.setBackground(new Color(0, 150, 0));
        header.setBorder(BorderFactory.createEmptyBorder(25, 0, 25, 0));
        
        JLabel headerLabel = new JLabel("Trip Completed!");
        headerLabel.setFont(new Font("Arial", Font.BOLD, 24));
        headerLabel.setForeground(Color.WHITE);
        header.add(headerLabel);
        
        JPanel paymentPanel = new JPanel(new GridBagLayout());
        paymentPanel.setBackground(Color.WHITE);
        paymentPanel.setBorder(BorderFactory.createEmptyBorder(40, 50, 40, 50));
        
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(15, 10, 15, 10);
        gbc.anchor = GridBagConstraints.WEST;
        
        gbc.gridx = 0; gbc.gridy = 0;
        JLabel summaryTitle = new JLabel("Trip Summary");
        summaryTitle.setFont(new Font("Arial", Font.BOLD, 16));
        paymentPanel.add(summaryTitle, gbc);
        
        gbc.gridy = 1;
        gbc.insets = new Insets(10, 10, 5, 10);
        paymentPanel.add(new JLabel("Rider: " + currentUser), gbc);
        
        gbc.gridy = 2;
        paymentPanel.add(new JLabel("From: " + pickupLocation), gbc);
        
        gbc.gridy = 3;
        paymentPanel.add(new JLabel("To: " + destinationLocation), gbc);
        
        gbc.gridy = 4;
        paymentPanel.add(new JLabel("Distance: " + String.format("%.1f", tripDistance) + " km"), gbc);
        
        gbc.gridy = 5;
        paymentPanel.add(new JLabel("Rate: $" + String.format("%.2f", tripRate) + " per km"), gbc);
        
        gbc.gridy = 6;
        gbc.insets = new Insets(15, 10, 5, 10);
        JLabel driverInfo = new JLabel("Driver: " + assignedDriver + " (" + assignedDriverVehicle + ")");
        driverInfo.setFont(new Font("Arial", Font.BOLD, 14));
        paymentPanel.add(driverInfo, gbc);
        
        gbc.gridy = 7;
        gbc.insets = new Insets(5, 10, 5, 10);
        paymentPanel.add(new JLabel("Phone: " + assignedDriverPhone), gbc);
        
        gbc.gridy = 8;
        paymentPanel.add(new JLabel("Rating: " + assignedDriverRating + "/5.0 *"), gbc);
        
        gbc.gridy = 9;
        gbc.insets = new Insets(30, 10, 15, 10);
        JLabel fareTitle = new JLabel("Fare Breakdown");
        fareTitle.setFont(new Font("Arial", Font.BOLD, 16));
        paymentPanel.add(fareTitle, gbc);
        
        // Calculate real fare components
        double baseFare = tripDistance * tripRate;
        double surgeAmount = currentFare - baseFare;
        double surgeMultiplier = currentFare / baseFare;
        
        gbc.gridy = 10;
        gbc.insets = new Insets(10, 10, 5, 10);
        paymentPanel.add(new JLabel("Base Fare (" + String.format("%.1f", tripDistance) + "km × $" + String.format("%.2f", tripRate) + "): $" + String.format("%.2f", baseFare)), gbc);
        
        gbc.gridy = 11;
        paymentPanel.add(new JLabel("Surge Pricing (" + String.format("%.1fx", surgeMultiplier) + "): $" + String.format("%.2f", surgeAmount)), gbc);
        
        gbc.gridy = 12;
        gbc.insets = new Insets(20, 10, 15, 10);
        JLabel totalLabel = new JLabel("Total: $" + String.format("%.2f", currentFare));
        totalLabel.setFont(new Font("Arial", Font.BOLD, 18));
        totalLabel.setForeground(new Color(0, 150, 0));
        paymentPanel.add(totalLabel, gbc);
        
        gbc.gridy = 13;
        gbc.insets = new Insets(30, 10, 10, 10);
        JLabel paymentLabel = new JLabel("Payment Method");
        paymentLabel.setFont(new Font("Arial", Font.BOLD, 14));
        paymentPanel.add(paymentLabel, gbc);
        
        JComboBox<String> paymentBox = new JComboBox<>(new String[]{"**** 1234 (Default Card)", "PayPal", "Cash"});
        gbc.gridy = 14;
        gbc.insets = new Insets(5, 10, 15, 10);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        paymentPanel.add(paymentBox, gbc);
        
        JPanel buttonPanel = new JPanel(new FlowLayout());
        buttonPanel.setBackground(Color.WHITE);
        
        JButton payBtn = new JButton("Pay Now");
        payBtn.setBackground(new Color(0, 150, 0));
        payBtn.setForeground(Color.WHITE);
        payBtn.setFont(new Font("Arial", Font.BOLD, 16));
        
        JButton rateBtn = new JButton("Rate Driver");
        rateBtn.setBackground(Color.LIGHT_GRAY);
        
        payBtn.addActionListener(e -> {
            String method = (String) paymentBox.getSelectedItem();
            processPayment(method);
        });
        
        rateBtn.addActionListener(e -> {
            String[] ratings = {"1 Star", "2 Stars", "3 Stars", "4 Stars", "5 Stars"};
            String rating = (String) JOptionPane.showInputDialog(this, 
                "Rate " + assignedDriver + ":", "Driver Rating",
                JOptionPane.QUESTION_MESSAGE, null, ratings, ratings[4]);
            if (rating != null) {
                log("Driver " + assignedDriver + " rated: " + rating);
                JOptionPane.showMessageDialog(this, "Thank you for your feedback!");
            }
        });
        
        buttonPanel.add(payBtn);
        buttonPanel.add(rateBtn);
        
        gbc.gridy = 15;
        gbc.insets = new Insets(30, 10, 15, 10);
        paymentPanel.add(buttonPanel, gbc);
        
        panel.add(header, BorderLayout.NORTH);
        panel.add(paymentPanel, BorderLayout.CENTER);
        
        // Replace the payment panel and show it
        mainPanel.remove(mainPanel.getComponent(4)); // Remove old payment panel
        mainPanel.add(panel, "PAYMENT", 4); // Add new panel at index 4
        cardLayout.show(mainPanel, "PAYMENT");
    }
    
    private void connectToServer() {
        try {
            socket = new Socket("localhost", 8080);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            log("Connected to Uber server");
        } catch (IOException e) {
            log("Server connection failed: " + e.getMessage());
            JOptionPane.showMessageDialog(this, "Cannot connect to server. Make sure UberServer is running.");
        }
    }
    
    private void sendRequest(String request) {
        if (out != null) {
            String fullRequest = request + ";" + (++localTime);
            out.println(fullRequest);
            log("Sent: " + fullRequest);
            
            try {
                String response = in.readLine();
                if (response != null) {
                    log("Response: " + response);
                    
                    // Parse driver assignment response
                    if (request.startsWith("ASSIGN_DRIVER") && response.startsWith("SUCCESS|")) {
                        parseDriverInfo(response);
                    }
                }
            } catch (IOException e) {
                log("Error reading response: " + e.getMessage());
            }
        }
    }
    
    private void parseDriverInfo(String response) {
        // Format: SUCCESS|DriverName|Vehicle|Phone|Rating|Location
        String[] parts = response.split("\\|");
        if (parts.length >= 6) {
            assignedDriver = parts[1];
            assignedDriverVehicle = parts[2];
            assignedDriverPhone = parts[3];
            assignedDriverRating = parts[4];
            String location = parts[5];
            log("Driver assigned: " + assignedDriver + " (" + assignedDriverVehicle + ") at " + location);
        }
    }
    
    private void processPayment(String method) {
        if (currentRideId != null) {
            sendRequest("payment;" + currentRideId + ";" + method);
            log("Payment processed: " + method + " - $" + String.format("%.2f", currentFare));

            
            JOptionPane.showMessageDialog(this, "Payment successful!\nThank you for riding with Uber!\nAmount: $" + 
                       String.format("%.2f", currentFare));
            
            currentRideId = null;
            currentFare = 0.0;
            
            Timer timer = new Timer(2000, e -> cardLayout.show(mainPanel, "BOOKING"));
            timer.setRepeats(false);
            timer.start();
        }
    }
    
    private void showHDFSStatus() {
        if (out != null) {
            sendRequest("HDFS_STATUS");
            sendRequest("HDFS_LIST_RIDES");
            sendRequest("HDFS_LIST_DRIVERS");
            
            JOptionPane.showMessageDialog(this, 
                "HDFS Status requested.\nCheck system log for details.\n\n" +
                "HDFS Features:\n" +
                "• Distributed file storage\n" +
                "• Automatic ride data backup\n" +
                "• Driver information storage\n" +
                "• GPS tracking logs\n" +
                "• Fault-tolerant replication", 
                "Uber HDFS Status", 
                JOptionPane.INFORMATION_MESSAGE);
        }
    }
    
    private void showFaultToleranceMenu() {
        if (out == null) {
            JOptionPane.showMessageDialog(this, "Not connected to server");
            return;
        }
        
        String[] options = {
            "Check Health Status",
            "Check Backup Status", 
            "Simulate Node Failure",
            "Simulate Network Partition",
            "Recover from Partition",
            "Cancel"
        };
        
        int choice = JOptionPane.showOptionDialog(this,
            "Choose a fault tolerance operation:\n\n" +
            "WARNING: Simulation operations will affect server behavior!\n" +
            "Check the backend terminal for detailed fault tolerance logs.",
            "Fault Tolerance Control Panel",
            JOptionPane.DEFAULT_OPTION,
            JOptionPane.QUESTION_MESSAGE,
            null,
            options,
            options[0]);
            
        switch (choice) {
            case 0: // Health Status
                sendRequest("HEALTH_STATUS");
                log("Health status check requested");
                break;
                
            case 1: // Backup Status
                sendRequest("BACKUP_STATUS");
                log("Backup status check requested");
                break;
                
            case 2: // Simulate Node Failure
                String nodeId = JOptionPane.showInputDialog(this, 
                    "Enter node ID to fail (1-5):", "Simulate Node Failure", 
                    JOptionPane.QUESTION_MESSAGE);
                if (nodeId != null && !nodeId.trim().isEmpty()) {
                    try {
                        int id = Integer.parseInt(nodeId.trim());
                        if (id >= 1 && id <= 5) {
                            sendRequest("SIMULATE_FAILURE;" + id);
                            log("Simulating failure of node " + id);
                            JOptionPane.showMessageDialog(this, 
                                "Node " + id + " failure simulated!\n" +
                                "Check backend terminal for fault tolerance response.");
                        } else {
                            JOptionPane.showMessageDialog(this, "Node ID must be between 1 and 5");
                        }
                    } catch (NumberFormatException e) {
                        JOptionPane.showMessageDialog(this, "Invalid node ID");
                    }
                }
                break;
                
            case 3: // Simulate Network Partition
                int confirm = JOptionPane.showConfirmDialog(this,
                    "This will simulate a network partition by disconnecting half the nodes.\n" +
                    "Are you sure you want to continue?",
                    "Confirm Network Partition Simulation",
                    JOptionPane.YES_NO_OPTION,
                    JOptionPane.WARNING_MESSAGE);
                    
                if (confirm == JOptionPane.YES_OPTION) {
                    sendRequest("SIMULATE_PARTITION");
                    log("Simulating network partition");
                    JOptionPane.showMessageDialog(this, 
                        "Network partition simulated!\n" +
                        "Check backend terminal for fault tolerance response.");
                }
                break;
                
            case 4: // Recover from Partition
                sendRequest("RECOVER_PARTITION");
                log("Initiating partition recovery");
                JOptionPane.showMessageDialog(this, 
                    "Partition recovery initiated!\n" +
                    "Check backend terminal for recovery progress.");
                break;
                
            case 5: // Cancel
            default:
                break;
        }
    }
    
    private void log(String message) {
        SwingUtilities.invokeLater(() -> {
            logArea.append(message + "\n");
            logArea.setCaretPosition(logArea.getDocument().getLength());
        });
    }
    
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            new UberGUI().setVisible(true);
        });
    }
}
