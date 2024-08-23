This project contains implementation of simple SQL database for learning purposes. Current implementation
limitations:
* Out-of-the box TreeMap is used for indexing currently for simplicity
* Effective storage format is not considered. CSV is used for simplicity as an initial implementation
* Row index is limited by int type due to CSVReader constructor allow skip lines parameters as int only
* No outside interface. Only unit tests are available

Includes:
* Basic operations supports:
  * CREATE TABLE - TODO
  * DROP TABLE - TODO
  * INSERT - TODO
  * UPDATE - TODO
  * SELECT - TODO
  * DELETE - TODO
* Index structure - fault-tolerance and durability - TODO
* Transactions support - TODO
* Secondary indexes -TODO