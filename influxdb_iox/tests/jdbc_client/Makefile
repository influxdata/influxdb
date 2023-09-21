driver_path := org/apache/arrow/flight-sql-jdbc-driver/10.0.0/flight-sql-jdbc-driver-10.0.0.jar
driver_file := $(notdir $(driver_path))

Main.class: Main.java
	javac Main.java

$(driver_file): $(driver_file).sha
	curl -s -L "https://search.maven.org/remotecontent?filepath=$(driver_path)" -o $(driver_file)
	shasum -c $(driver_file).sha

clean:
	rm -f $(driver_file)
	rm -f Main.class
