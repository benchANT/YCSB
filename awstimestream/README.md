<!--
Copyright (c) 2018 YCSB contributors.
Copyright (c) 2023 - 2024, benchANT GmbH. All rights reserved.
All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

### Required environment variables

- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY


#### Options parameters

- timestream.database < name string > :
    - Name of the database to use.
    - Default: ""

- timestream.region < name string > :
  - Name of the region that the instance is located in.
  - Default: ""

- timestream.inittable (true | false) :
  - Describes if the table should be initialised
  - Default: false
