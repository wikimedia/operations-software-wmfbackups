-- MariaDB dump 10.17  Distrib 10.4.13-MariaDB, for Linux (x86_64)
--
-- Host: db1133.eqiad.wmnet    Database: mediabackups
-- ------------------------------------------------------
-- Server version       10.4.14-MariaDB-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `backup_status`
--

DROP TABLE IF EXISTS `backup_status`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `backup_status` (
  `id` tinyint(3) unsigned NOT NULL,
  `backup_status_name` varbinary(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `backup_status`
--

LOCK TABLES `backup_status` WRITE;
/*!40000 ALTER TABLE `backup_status` DISABLE KEYS */;
INSERT INTO `backup_status` VALUES (1,'pending'),(2,'processing'),(3,'backedup'),(4,'error');
/*!40000 ALTER TABLE `backup_status` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

--
-- Table structure for table `file_status`
--

DROP TABLE IF EXISTS `file_status`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `file_status` (
  `id` tinyint(3) unsigned NOT NULL AUTO_INCREMENT,
  `status_name` varbinary(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=binary;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `file_status`
--

LOCK TABLES `file_status` WRITE;
/*!40000 ALTER TABLE `file_status` DISABLE KEYS */;
INSERT INTO `file_status` VALUES (1,'public'),(2,'archived'),(3,'deleted'),(4,'hard-deleted');
/*!40000 ALTER TABLE `file_status` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `file_types`
--

DROP TABLE IF EXISTS `file_types`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `file_types` (
  `id` tinyint(3) unsigned NOT NULL AUTO_INCREMENT,
  `Type_name` varbinary(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=14 DEFAULT CHARSET=binary;
/*!40101 SET character_set_client = @saved_cs_client */;

LOCK TABLES `file_types` WRITE;
/*!40000 ALTER TABLE `file_types` DISABLE KEYS */;
INSERT INTO `file_types` VALUES (0,'ERROR'),(1,'UNKNOWN'),(2,'BITMAP'),(3,'DRAWING'),(4,'AUDIO'),(5,'VIDEO'),(6,'MULTIMEDIA'),(7,'OFFICE'),(8,'TEXT'),(9,'EXECUTABLE'),(10,'ARCHIVE'),(11,'3D');
/*!40000 ALTER TABLE `file_types` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `swift_container_types`
--

DROP TABLE IF EXISTS `swift_container_types`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `swift_container_types` (
  `id` tinyint(3) unsigned NOT NULL AUTO_INCREMENT,
  `swift_container_type_name` varbinary(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=binary;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `swift_container_types`
--

LOCK TABLES `swift_container_types` WRITE;
/*!40000 ALTER TABLE `swift_container_types` DISABLE KEYS */;
INSERT INTO `swift_container_types` VALUES (1,'originals-public'),(2,'originals-deleted');
/*!40000 ALTER TABLE `swift_container_types` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `wiki_types`
--

DROP TABLE IF EXISTS `wiki_types`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `wiki_types` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `type_name` varbinary(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=binary;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `wiki_types`
--

LOCK TABLES `wiki_types` WRITE;
/*!40000 ALTER TABLE `wiki_types` DISABLE KEYS */;
INSERT INTO `wiki_types` VALUES (1,'public'),(2,'private'),(3,'deleted'),(4,'closed');
/*!40000 ALTER TABLE `wiki_types` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `wikis`
--

DROP TABLE IF EXISTS `wikis`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `wikis` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `wiki_name` varbinary(255) DEFAULT NULL,
  `type` tinyint(3) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `wiki_name` (`wiki_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1206 DEFAULT CHARSET=binary;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `swift_containers`
--

DROP TABLE IF EXISTS `swift_containers`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `swift_containers` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `swift_container_name` varbinary(270) NOT NULL,
  `wiki` int(10) unsigned DEFAULT NULL,
  `type` tinyint(3) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `swift_container_name` (`swift_container_name`),
  KEY `wiki` (`wiki`),
  KEY `type` (`type`),
  CONSTRAINT `swift_containers_ibfk_1` FOREIGN KEY (`wiki`) REFERENCES `wikis` (`id`),
  CONSTRAINT `swift_containers_ibfk_2` FOREIGN KEY (`type`) REFERENCES `swift_container_types` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=86679 DEFAULT CHARSET=binary;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `files`
--

DROP TABLE IF EXISTS `files`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `files` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `wiki` int(10) unsigned NOT NULL,
  `upload_name` varbinary(255) DEFAULT NULL,
  `swift_container` int(10) unsigned DEFAULT NULL,
  `swift_name` varbinary(270) DEFAULT NULL,
  `file_type` tinyint(3) unsigned DEFAULT NULL,
  `status` tinyint(3) unsigned DEFAULT NULL,
  `sha1` varbinary(40) DEFAULT NULL,
  `md5` varbinary(32) DEFAULT NULL,
  `size` int(10) unsigned DEFAULT NULL,
  `upload_timestamp` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `archived_timestamp` timestamp NULL DEFAULT NULL,
  `deleted_timestamp` timestamp NULL DEFAULT NULL,
  `backup_status` tinyint(3) unsigned DEFAULT 1,
  PRIMARY KEY (`id`),
  KEY `sha1` (`sha1`),
  KEY `file_type` (`file_type`),
  KEY `status` (`status`),
  KEY `backup_status` (`backup_status`),
  KEY `wiki` (`wiki`),
  KEY `swift_container` (`swift_container`),
  KEY `upload_name` (`upload_name`,`status`),
  KEY `upload_timestamp` (`upload_timestamp`),
  CONSTRAINT `files_ibfk_1` FOREIGN KEY (`file_type`) REFERENCES `file_types` (`id`),
  CONSTRAINT `files_ibfk_2` FOREIGN KEY (`status`) REFERENCES `file_status` (`id`),
  CONSTRAINT `files_ibfk_3` FOREIGN KEY (`wiki`) REFERENCES `wikis` (`id`),
  CONSTRAINT `files_ibfk_4` FOREIGN KEY (`backup_status`) REFERENCES `backup_status` (`id`),
  CONSTRAINT `files_ibfk_5` FOREIGN KEY (`swift_container`) REFERENCES `swift_containers` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4549852 DEFAULT CHARSET=binary;
/*!40101 SET character_set_client = @saved_cs_client */;


/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2020-12-17 19:51:26
