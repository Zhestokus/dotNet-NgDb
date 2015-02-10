# dotNet-NgDb

NgDb is Database engine writen on C#,
NgDb can be fully distributed 
(you can change location of any object of database 
e.g developer can store every Table/Column/Index on different Disk/HDD or anywhere he/she want, 
developer need just implement IDbStorage interface which returns Stream object for Db Object (table/column/index))
or it can be InMemory DB (as i mentioned above developer need just implement IDbStorage interface)
NgDb is using BPlusTree for data indexing and BPlusTree is using Index Sort method which means Data is not duplicated in Index, 
Index just contains row indices in columns because of why index do not need more/same space then real data

NOTE:
Don't be a critical,
this is just draft, i don't know is it fully workable or not 
but for test data (see Program.cs file) it working fine
