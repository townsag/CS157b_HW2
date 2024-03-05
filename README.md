# CS157b_HW2
Contributers: Andrew Townsend and Peter Guilhamet

This is what my directory structure looks like:
.
├── Database.class
├── Database.java
├── PHTManager.class
├── PHTManager.java
├── README.md
├── Util.java
├── instructions.txt
├── lib
│   ├── slf4j-api-1.7.36.jar
│   └── sqlite-jdbc-3.45.1.0.jar
└── test.db

This is what I'm using to compile and run the code:
javac -classpath lib/sqlite-jdbc-3.45.1.0.jar:lib/slf4j-api-1.7.36.jar:. PHTManager.java
java -classpath lib/sqlite-jdbc-3.45.1.0.jar:lib/slf4j-api-1.7.36.jar:. PHTManager test.db instructions.txt
