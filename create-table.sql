CREATE TABLE `weetabix` (
  `CREATED_DATE` varchar(50) NOT NULL,
  `DCN_YEAR` int(11) NOT NULL,
  `DCN_DCN` int(11) NOT NULL,
  `DCN_REV` int(11) NOT NULL,
  `TITLE` varchar(200) NOT NULL,
  `AUTH_AFFIL` varchar(200) NOT NULL,
  `GROOP` varchar(45) NOT NULL,
  `UPLOADED_DATE` varchar(45) NOT NULL,
  `FILENAME` varchar(300) NOT NULL,
  PRIMARY KEY (`DCN_YEAR`,`DCN_DCN`,`DCN_REV`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='scraped from IEEE web site';
