# Install Maven if you haven't already
# For Ubuntu/Debian:
# sudo apt-get install maven

# For macOS with Homebrew:
# brew install maven

# Compile and package
mvn clean package

# Run the program (replace with your SST file path)
java -jar target/rocksdemo-1.0-SNAPSHOT-jar-with-dependencies.jar ../test.db/000009.sst
