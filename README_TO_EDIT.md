# OOD
Object Oriented Database Using HSQLDB

# Changes

[See historical of changes](./versions.md)

# How to use it ?
## With Gradle :

Adapt into your build.gradle file, the next code :

	...
	repositories {
		...
		maven {
	       		url "https://artifactory.distri-mind.fr/artifactory/gradle-release"
	   	}
		...
	}
	...
	dependencies {
		...
		compile(group:'com.distrimind.ood', name: 'OOD', version: '//PROJECT_VERSION//')
		//choose one of these optional drivers for H2 database
			//optional and under JDK7 or Android API21
			testImplementation(group:'com.h2database', name: 'h2', version: '1.4.199')
			//optional and under JDK8 or newer
			testImplementation(group:'com.h2database', name: 'h2', version: '1.4.200')
		//choose one of these optional drivers for HSQLDB
			//optional and under JDK7 
			testImplementation(group:'org.hsqldb', name: 'hsqldb', version: '2.3.4')
			//optional and under JDK8 or newer
			testImplementation(group:'org.hsqldb', name: 'hsqldb', version: '2.5.0')
		//choose one of these optional drivers for DerbyDB
			//optional and under JDK7 
			testImplementation(group:'org.apache.derby', name: 'derby', version: '10.11.1.1')
			//optional and under JDK8 or newer
			testImplementation(group:'org.apache.derby', name: 'derby', version: '10.15.1.3')
		//choose this optional driver for MySQL 
			testImplementation(group: 'mysql', name: 'mysql-connector-java', version: '8.0.19')

		...
	}
	...


To know what last version has been uploaded, please refer to versions availables into [this repository](https://artifactory.distri-mind.fr/artifactory/DistriMind-Public/com/distrimind/ood/OOD/)
## With Maven :
Adapt into your pom.xml file, the next code :

	<project>
		...
		<dependencies>
			...
			<dependency>
				<groupId>com.distrimind.ood</groupId>
				<artifactId>OOD</artifactId>
				<version>//PROJECT_VERSION//</version>
			</dependency>
			<!-- choose one of these optional drivers for H2 database-->
				<dependency>
					<groupId>com.h2database</groupId>
					<artifactId>h2</artifactId>
	
					<!-- under JDK 7 or Android API21-->
					<version>1.4.199</version>

					<!-- under JDK 8 or newer -->
					<version>1.4.200</version>
				</dependency>

			<!-- choose one of these optional drivers for HSQLDB-->
				<dependency>
					<groupId>org.hsqldb</groupId>
					<artifactId>hsqldb</artifactId>
	
					<!-- under JDK 8 or newer -->
					<version>2.5.0</version>
	
					<!-- under JDK 7 or newer -->
					<version>2.3.4</version>
				</dependency>
			<!-- choose one of these optional drivers for DerbyDB-->
				<dependency>
					<groupId>org.apache.derby</groupId>
					<artifactId>derby</artifactId>
	
					<!-- under JDK 8 or newer -->
					<version>10.15.1.3</version>
	
					<!-- under JDK 7 or newer -->
					<version>10.11.1.1</version>
				</dependency>
			<!-- choose this optional driver for MySQL-->
				<dependency>
					<groupId>mysql</groupId>
					<artifactId>mysql-connector-java</artifactId>
					<version>8.0.19</version>
				</dependency>
			...
		</dependencies>
		...
		<repositories>
			...
			<repository>
				<id>DistriMind-Public</id>
				<url>https://artifactory.distri-mind.fr/artifactory/gradle-release</url>
			</repository>
			...
		</repositories>
	</project>

To know what last version has been uploaded, please refer to versions availables into [this repository](https://artifactory.distri-mind.fr/artifactory/DistriMind-Public/com/distrimind/ood/OOD/)
