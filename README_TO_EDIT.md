# OOD

[![CodeQL](https://github.com/JazZ51/OOD/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/JazZ51/OOD/actions/workflows/codeql-analysis.yml)

OOD is a decentralized and asynchrone ORM (Object-Relational Mapping):
* Compatible with Java 8 and newer
* drivers for MySQL, PostgreSQL, H2, H2 for Android, HSQLDB
* Transparent decentralized and secured database synchronization
  * Automatically synchronize database with other authorized peers, even if peers where disconnected (asynchronous mode).
  * Manage eventual conflicts during database synchronization
  * Automatically backup database and it historical to authorized servers and use end-to-end encryption : the servers are not able to read database in clear. 
  * Autotatically synchronize peers through server side database backup if peers are not connected (asynchronous mode)
  * Permit to revert database to an old version with a given time, and synchronize the restoration toward others peers and servers. It is possible to choose data source from local backup, other peers backup, or server backup.
  * Backups cannot be removed into servers from peers for a certain period. They cannot also be removed from a peer into a distant peer. This permit to garuantee the restoration even after an attack of a ransomware. 
  * Permit to restore database of peers thanks to other peers or thanks to servers
  * Automatically synchronize peers though intermediate peers when direct connections are not possible
  * Use encryption profile provider and to manage different version of secret keys in the case where a secret key was compromised
* pseudo SQL querries possible (where clause)



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
		testImplementation(group:'com.h2database', name: 'h2', version: '1.4.200')
		//optional and under JDK8 or newer
		testImplementation(group:'org.hsqldb', name: 'hsqldb', version: '2.6.0')
		//choose this optional driver for MySQL
		testImplementation(group: 'mysql', name: 'mysql-connector-java', version: '8.0.26')
		//choose this optional driver for PostgreSQL
		testImplementation(group: 'org.postgresql', name: 'postgresql', version: '42.2.23')
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

					<!-- under JDK 8 or newer -->
					<version>1.4.200</version>
				</dependency>

			<!-- choose one of these optional drivers for HSQLDB-->
				<dependency>
					<groupId>org.hsqldb</groupId>
					<artifactId>hsqldb</artifactId>

					<!-- under JDK 8 or newer -->
					<version>2.6.0</version>
				</dependency>
			<!-- choose this optional driver for MySQL-->
				<dependency>
					<groupId>mysql</groupId>
					<artifactId>mysql-connector-java</artifactId>
					<version>8.0.26</version>
				</dependency>
			<!-- choose this optional driver for PostgreSQL-->
				<dependency>
					<groupId>org.postgresql</groupId>
					<artifactId>postgresql</artifactId>
					<version>42.2.23</version>
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

###### Requirements under Ubuntu/Debian :
  * Please install the package ethtool, rng-tools, mtr(only debian)
