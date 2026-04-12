# Real-Time IoT Workload (RtIotWorkload)

## Configuration Parameters

The RtIotWorkload requires several configuration properties to be passed during initialization. These parameters control the behavior of the real-time IoT simulation.

### Parameters

#### `riot.start_time`

*   **Type:** ISO 8601 Timestamp
*   **Default:** `2026-01-01T00:00:00Z`
*   **Description:** The start time for the workload simulation. Must be in ISO 8601 format (e.g., `2026-01-01T00:00:00Z`).
*   **Example:** `riot.start_time=2026-01-01T00:00:00Z`

#### `riot.stop_time`

*   **Type:** ISO 8601 Timestamp
*   **Default:** `2026-03-31T23:59:59Z`
*   **Description:** The end time for the workload simulation. Must be in ISO 8601 format and must be after the start time.
*   **Validation:** Must be greater than `riot.start_time`
*   **Example:** `riot.stop_time=2026-03-31T23:59:59Z`

#### `riot.machines`

*   **Type:** Integer
*   **Default:** `10000`
*   **Description:** The number of sensor devices/machines in the simulation. Each machine generates data at the specified machine frequency.
*   **Constraints:** Must be greater than 0
*   **Example:** `riot.machines=5000`

#### `riot.senders`

*   **Type:** Integer
*   **Default:** `200`
*   **Description:** The number of proxy servers between the machines and the database. Senders batch and forward data from machines to the database.
*   **Constraints:** Must be greater than 0
*   **Example:** `riot.senders=100`

#### `riot.machine_frequency`

*   **Type:** Integer
*   **Default:** `15`
*   **Description:** The frequency (in seconds) at which each machine generates a data element. For example, a value of 15 means each machine generates data every 15 seconds.
*   **Constraints:** Must be greater than 0
*   **Example:** `riot.machine_frequency=30`

#### `riot.sender_frequency`

*   **Type:** Integer
*   **Default:** `60`
*   **Description:** The frequency (in seconds) at which senders issue batches of data elements to the database. For example, a value of 60 means batches are sent every 60 seconds.
*   **Constraints:** Must be greater than 0
*   **Example:** `riot.sender_frequency=120`

## Usage Example

When running the YCSB workload, pass these properties as parameters:

\`\`\`bash  
./bin/ycsb run iotdb   
\-P workloads/workload\_iot   
\-p riot.start\_time=2026-01-01T00:00:00Z   
\-p riot.stop\_time=2026-03-31T23:59:59Z   
\-p riot.machines=5000   
\-p riot.senders=100   
\-p riot.machine\_frequency=15   
\-p riot.sender\_frequency=60