# Record Router

<img  alt="Cask Market Availability" src="https://cdap-users.herokuapp.com/assets/cm-notavailable.svg"/> <a href="https://cdap-users.herokuapp.com/"><img alt="Join CDAP community" src="https://cdap-users.herokuapp.com/badge.svg?t=1"/></a> ![cdap-transform](https://cdap-users.herokuapp.com/assets/cdap-transform.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Join CDAP community](https://cdap-users.herokuapp.com/badge.svg?t=wrangler)](https://cdap-users.herokuapp.com?t=1)

This is a splitter transform plugin for [CDAP Pipelines](https://docs.cask.co/cdap/current/en/developer-manual/pipelines/index.html), that can help you route records to a given output port, based on predefined rules.
 
It is fully open source under the Apache 2.0 license.

## Documentation

The docs for the plugin can be found at [Record Router](docs/RecordRouter-splittertransform.md).

## Need Help?

### Mailing Lists

CDAP User Group and Development Discussions:

- [cdap-user](https://groups.google.com/d/forum/cdap-user)

The *cdap-user* mailing list is primarily for users using the product to develop
applications. You can expect questions from users, release announcements, and any other
discussions that we think will be helpful to the users.

- [cdap-dev](https://groups.google.com/d/forum/cdap-dev)

The *cdap-dev* mailing list is essentially for developers actively working
on the product, and should be used for all our design, architecture and technical
discussions moving forward. This mailing list will also receive all JIRA and GitHub
notifications.

## Build

To build the plugin:

    mvn clean package
    
You can also build without running tests: 

    mvn clean package -DskipTests

The build will create a .jar and .json file under the ``target`` directory.
These files can be uploaded to CDAP to deploy your plugins.

## Deployment

You can deploy the plugin using the UI, REST API or CLI.

## License and Trademarks

Copyright © 2019 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
