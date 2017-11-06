# Vehicle Routing Challenge

A school is trying to deploy bus routing software to centralize student transportation records in a central database. To help school with our technical skills we paired the routing program with in-bus GPS system for managing bus fleet. We will be using data collection to allow us to lower daily costs, boost safety and potentially even increase state funding returns.

Your mission is to collect incoming bus dongle messages in JSON format from the Kafka queue and fulfill Functional Requirements by posting rest based message to a service called Trips in a given Output JSON format.

## Kafka Input Message (JSON)

    {
      "dongleId"               : [string] UUID: Unique device identifier,
      "driverId"               : [string] UUID: Unique agent identifier,
      "busId"                  : [string] UUID Unique vehicle identifier,
      "driverPhoneId"          : [string] UUID: Phone identifier, // optional
      "eventTime"              : [string] Event time and date in ISO 8601,
      "lat"                    : [float] latitude,
      "long"                   : [float] longitude,
      "eventId"                : [string] UUID: Unique identifier of this event,
      "eventType"              : [string] Event type. One of: 'location',
      "speed"                  : [float] km/h, // optional
      "heading"                : [float] bearing in radians, // optional
      "fuel"                   : [float] fuel percentage, // optional
      "mileage"                : [double] Current mileage reading in km, // optional
      "battery"                : [float] Resting battery voltage, // optional
      "gForce"                 : [float] g, // optional
      "speedChange"            : [float] km/h // optional 
    }
    
## Functional Requirements

1. Count total hard brakes in a trip
2. Count total speeding instances and speed at that time
3. Keep track of all bus stops locations
4. Fuel consumption in a trip
5. Distance covered in a trip
