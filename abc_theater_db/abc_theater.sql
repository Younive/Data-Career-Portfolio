-- phpMyAdmin SQL Dump
-- version 5.2.0
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Generation Time: Sep 02, 2022 at 04:22 AM
-- Server version: 10.4.24-MariaDB
-- PHP Version: 8.1.6

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
CREATE DATABASE `abc_theater`;

USE `abc_theater`;
--
-- Database: `abc_theater`
--

-- --------------------------------------------------------

--
-- Table structure for table `hall`
--

CREATE TABLE `hall` (
  `hall_id` varchar(3) NOT NULL,
  `hall_type` varchar(10) NOT NULL DEFAULT 'Normal'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table `hall`
--

INSERT INTO `hall` (`hall_id`, `hall_type`) VALUES
('T1', 'Normal'),
('T2', 'Normal'),
('T3', '3D'),
('T4', '4D'),
('T5', 'IMAX');

-- --------------------------------------------------------

--
-- Table structure for table `member`
--

CREATE TABLE `member` (
  `member_id` int(5) UNSIGNED ZEROFILL NOT NULL,
  `member_name` varchar(20) NOT NULL,
  `member_pw` char(8) NOT NULL,
  `email` varchar(20) NOT NULL,
  `phone` char(12) NOT NULL,
  `birth_date` date NOT NULL,
  `role` varchar(5) NOT NULL DEFAULT 'user' CHECK (`role` in ('user','admin'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table `member`
--

INSERT INTO `member` (`member_id`, `member_name`, `member_pw`, `email`, `phone`, `birth_date`, `role`) VALUES
(00001, 'Suratcha Kamolpanus', 'p21jrU6y', 'Suratcha@gmail.com', '092-092-3421', '2001-09-11','admin'),
(00002, 'Siriwut Thongmak', '85av3DCj', 'Siriwut@gmail.com', '087-162-5492', '2001-04-01','user'),
(00003, 'Nattakit Kongtan', '46ltQn1O', 'Nattakit@gmail.com', '096-959-3660', '2000-03-12','user'),
(00004, 'Kelechi Hidayat', 'Y276oW8P', 'Kelechi@gmail.com', '081-519-3485', '1994-10-01','user'),
(00005, 'Lionors Ibrahim', 'E90m4ayb', 'Lionors@gmail.com', '092-649-1525', '1999-08-10','user');

-- --------------------------------------------------------

--
-- Table structure for table `movie`
--

CREATE TABLE `movie` (
  `movie_id` int(5) UNSIGNED ZEROFILL NOT NULL,
  `movie_name` varchar(50) NOT NULL,
  `duration` int(11) NOT NULL,
  `get_in_date` date NOT NULL,
  `get_out_date` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table `movie`
--

INSERT INTO `movie` (`movie_id`, `movie_name`, `duration`, `get_in_date`, `get_out_date`) VALUES
(00001, 'Iron Man', 126, '2000-01-01', '2000-01-29'),
(00002, 'The Incredible Hulk', 112, '2000-01-15', '2000-02-12'),
(00003, 'Iron Man 2', 125, '2000-01-29', '2000-02-26'),
(00004, 'Thor', 114, '2000-02-12', '2000-03-11'),
(00005, 'Captain America: The First Avenger', 124, '2000-02-26', '2000-03-25'),
(00006, "Marvel's The Avengers", 143, '2000-03-11', '2000-04-08'),
(00007, 'Iron Man 3', 131, '2000-03-25', '2000-04-22'),
(00008, 'Thor: The Dark World', 112, '2000-04-08', '2000-05-06'),
(00009, 'Captain America: The Winter Soldier', 136, '2000-04-22', '2000-05-20'),
(00010, 'Guardians of the Galaxy', 122, '2000-05-06', '2000-06-03'),
(00011, 'Avengers: Age of Ultron', 141, '2000-05-20', '2000-06-17'),
(00012, 'Ant-Man', 117, '2000-06-03', '2000-07-01'),
(00013, 'Captain America: Civil War', 147, '2000-06-17', '2000-07-15'),
(00014, 'Doctor Strange', 115, '2000-07-01', '2000-07-29'),
(00015, 'Guardians of the Galaxy Vol. 2', 137, '2000-07-15', '2000-08-12'),
(00016, 'Spider-Man: Homecoming', 133, '2000-07-29', '2000-08-26'),
(00017, 'Thor: Ragnarok', 130, '2000-08-12', '2000-09-06'),
(00018, 'Black Panther', 134, '2000-08-26', '2000-09-23'),
(00019, 'Avengers: Infinity War', 149, '2000-09-09', NULL),
(00020, "Ant-Man and the Wasp", 118, '2000-09-23', NULL),
(00021, 'Captain Marvel', 124, '2000-10-07', NULL),
(00022, 'Avengers: Endgame', 181, '2000-10-21', NULL),
(00023, 'Spider-Man: Homecoming', 129, '2000-11-04', NULL);

-- --------------------------------------------------------

--
-- Table structure for table `seat`
--

CREATE TABLE `seat` (
  `seat_id` varchar(3) NOT NULL,
  `hall_id` varchar(3) NOT NULL,
  `tier_seat_id` varchar(2) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table `seat`
--

INSERT INTO `seat` (`seat_id`, `hall_id`, `tier_seat_id`) VALUES
('A01', 'T1', '1'),
('A01', 'T2', '1'),
('A01', 'T3', '1'),
('A01', 'T4', '1'),
('A01', 'T5', '1'),
('A10', 'T1', '1'),
('A10', 'T2', '1'),
('A10', 'T3', '1'),
('A10', 'T4', '1'),
('A10', 'T5', '1'),
('A02', 'T1', '1'),
('A02', 'T2', '1'),
('A02', 'T3', '1'),
('A02', 'T4', '1'),
('A02', 'T5', '1'),
('A03', 'T1', '1'),
('A03', 'T2', '1'),
('A03', 'T3', '1'),
('A03', 'T4', '1'),
('A03', 'T5', '1'),
('A04', 'T1', '1'),
('A04', 'T2', '1'),
('A04', 'T3', '1'),
('A04', 'T4', '1'),
('A04', 'T5', '1'),
('A05', 'T1', '1'),
('A05', 'T2', '1'),
('A05', 'T3', '1'),
('A05', 'T4', '1'),
('A05', 'T5', '1'),
('A06', 'T1', '1'),
('A06', 'T2', '1'),
('A06', 'T3', '1'),
('A06', 'T4', '1'),
('A06', 'T5', '1'),
('A07', 'T1', '1'),
('A07', 'T2', '1'),
('A07', 'T3', '1'),
('A07', 'T4', '1'),
('A07', 'T5', '1'),
('A08', 'T1', '1'),
('A08', 'T2', '1'),
('A08', 'T3', '1'),
('A08', 'T4', '1'),
('A08', 'T5', '1'),
('A09', 'T1', '1'),
('A09', 'T2', '1'),
('A09', 'T3', '1'),
('A09', 'T4', '1'),
('A09', 'T5', '1'),
('B01', 'T1', '2'),
('B01', 'T2', '2'),
('B01', 'T3', '2'),
('B01', 'T4', '2'),
('B01', 'T5', '2'),
('B10', 'T1', '2'),
('B10', 'T2', '2'),
('B10', 'T3', '2'),
('B10', 'T4', '2'),
('B10', 'T5', '2'),
('B02', 'T1', '2'),
('B02', 'T2', '2'),
('B02', 'T3', '2'),
('B02', 'T4', '2'),
('B02', 'T5', '2'),
('B03', 'T1', '2'),
('B03', 'T2', '2'),
('B03', 'T3', '2'),
('B03', 'T4', '2'),
('B03', 'T5', '2'),
('B04', 'T1', '2'),
('B04', 'T2', '2'),
('B04', 'T3', '2'),
('B04', 'T4', '2'),
('B04', 'T5', '2'),
('B05', 'T1', '2'),
('B05', 'T2', '2'),
('B05', 'T3', '2'),
('B05', 'T4', '2'),
('B05', 'T5', '2'),
('B06', 'T1', '2'),
('B06', 'T2', '2'),
('B06', 'T3', '2'),
('B06', 'T4', '2'),
('B06', 'T5', '2'),
('B07', 'T1', '2'),
('B07', 'T2', '2'),
('B07', 'T3', '2'),
('B07', 'T4', '2'),
('B07', 'T5', '2'),
('B08', 'T1', '2'),
('B08', 'T2', '2'),
('B08', 'T3', '2'),
('B08', 'T4', '2'),
('B08', 'T5', '2'),
('B09', 'T1', '2'),
('B09', 'T2', '2'),
('B09', 'T3', '2'),
('B09', 'T4', '2'),
('B09', 'T5', '2'),
('C01', 'T1', '3'),
('C01', 'T2', '3'),
('C01', 'T3', '3'),
('C01', 'T4', '3'),
('C01', 'T5', '3'),
('C10', 'T1', '3'),
('C10', 'T2', '3'),
('C10', 'T3', '3'),
('C10', 'T4', '3'),
('C10', 'T5', '3'),
('C02', 'T1', '3'),
('C02', 'T2', '3'),
('C02', 'T3', '3'),
('C02', 'T4', '3'),
('C02', 'T5', '3'),
('C03', 'T1', '3'),
('C03', 'T2', '3'),
('C03', 'T3', '3'),
('C03', 'T4', '3'),
('C03', 'T5', '3'),
('C04', 'T1', '3'),
('C04', 'T2', '3'),
('C04', 'T3', '3'),
('C04', 'T4', '3'),
('C04', 'T5', '3'),
('C05', 'T1', '3'),
('C05', 'T2', '3'),
('C05', 'T3', '3'),
('C05', 'T4', '3'),
('C05', 'T5', '3'),
('C06', 'T1', '3'),
('C06', 'T2', '3'),
('C06', 'T3', '3'),
('C06', 'T4', '3'),
('C06', 'T5', '3'),
('C07', 'T1', '3'),
('C07', 'T2', '3'),
('C07', 'T3', '3'),
('C07', 'T4', '3'),
('C07', 'T5', '3'),
('C08', 'T1', '3'),
('C08', 'T2', '3'),
('C08', 'T3', '3'),
('C08', 'T4', '3'),
('C08', 'T5', '3'),
('C09', 'T1', '3'),
('C09', 'T2', '3'),
('C09', 'T3', '3'),
('C09', 'T4', '3'),
('C09', 'T5', '3');

-- --------------------------------------------------------

--
-- Table structure for table `show_schedule`
--

CREATE TABLE `show_annoucement` (
  `annoucement_id` int(5) UNSIGNED ZEROFILL NOT NULL,
  `start_date` date NOT NULL,
  `movie_id` int(5) UNSIGNED ZEROFILL NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table `show_schedule`
--

INSERT INTO `show_annoucement` (`annoucement_id`, `start_date`, `movie_id`) VALUES
(00001, '2000-01-01', 00001),
(00002, '2000-01-15', 00002),
(00003, '2000-01-29', 00003),
(00004, '2000-02-12', 00004),
(00005, '2000-02-26', 00005),
(00006, '2000-03-11', 00006),
(00007, '2000-03-25', 00007),
(00008, '2000-04-08', 00008),
(00009, '2000-05-20', 00009),
(00010, '2000-06-03', 00010),
(00011, '2000-06-17', 00011),
(00012, '2000-07-01', 00012),
(00013, '2000-07-15', 00013),
(00014, '2000-07-29', 00014),
(00015, '2000-08-12', 00015),
(00016, '2000-08-26', 00016),
(00017, '2000-09-06', 00017),
(00018, '2000-09-23', 00018),
(00019, '2000-09-09', 00019),
(00020, '2000-09-23', 00020),
(00021, '2000-10-07', 00021),
(00022, '2000-10-21', 00022),
(00023, '2000-11-04', 00023);

-- --------------------------------------------------------

--
-- Table structure for table `show_time`
--

CREATE TABLE `show_time` (
  `annoucement_id` int(5) UNSIGNED ZEROFILL NOT NULL,
  `slot_time_id` int(3) UNSIGNED ZEROFILL NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table `show_time`
--

INSERT INTO `show_time` (`annoucement_id`, `slot_time_id`) VALUES
(00019, 011),
(00019, 012),
(00019, 013),
(00019, 021),
(00019, 022),
(00019, 023),
(00019, 024),
(00019, 031),
(00019, 043),
(00019, 044),
(00019, 051),
(00019, 052),
(00019, 053),
(00019, 061),
(00019, 062),
(00019, 063),
(00019, 064),
(00019, 071),
(00019, 072),
(00019, 073),
(00019, 081),
(00019, 082),
(00019, 083),
(00019, 084),
(00019, 091),
(00019, 092),
(00019, 093),
(00019, 094),
(00019, 101),
(00019, 102),
(00019, 103),
(00019, 104),
(00019, 111),
(00019, 112),
(00019, 113),
(00019, 114),
(00019, 121),
(00019, 122),
(00019, 123),
(00019, 124),
(00019, 142),
(00019, 143),
(00019, 144),
(00019, 151),
(00019, 152),
(00019, 171),
(00019, 172),
(00019, 173),
(00019, 174),
(00020, 011),
(00020, 012),
(00020, 013),
(00020, 021),
(00020, 022),
(00020, 023),
(00020, 024),
(00020, 031),
(00020, 032),
(00020, 033),
(00020, 041),
(00020, 042),
(00020, 043),
(00020, 053),
(00020, 061),
(00020, 062),
(00020, 063),
(00020, 064),
(00020, 071),
(00020, 072),
(00020, 073),
(00020, 081),
(00020, 082),
(00020, 083),
(00020, 094),
(00020, 101),
(00020, 102),
(00020, 103),
(00020, 104),
(00020, 111),
(00020, 112),
(00020, 113),
(00020, 114),
(00020, 121),
(00020, 122),
(00020, 123),
(00020, 124),
(00020, 131),
(00020, 132),
(00020, 133),
(00020, 134),
(00020, 141),
(00020, 142),
(00020, 143),
(00020, 144),
(00020, 151),
(00020, 152),
(00020, 153),
(00020, 154),
(00020, 161),
(00020, 162),
(00020, 163),
(00020, 164),
(00020, 171),
(00020, 172),
(00020, 173),
(00020, 174),
(00021, 011),
(00021, 012),
(00021, 013),
(00021, 021),
(00021, 022),
(00021, 023),
(00021, 024),
(00021, 031),
(00021, 032),
(00021, 033),
(00021, 041),
(00021, 042),
(00021, 043),
(00021, 044),
(00021, 051),
(00021, 052),
(00021, 053),
(00021, 061),
(00021, 062),
(00021, 063),
(00021, 064),
(00021, 091),
(00021, 092),
(00021, 093),
(00021, 094),
(00021, 101),
(00021, 102),
(00021, 103),
(00021, 104),
(00021, 111),
(00021, 112),
(00021, 113),
(00021, 114),
(00021, 121),
(00021, 122),
(00021, 123),
(00021, 132),
(00021, 133),
(00021, 134),
(00021, 141),
(00021, 142),
(00021, 143),
(00021, 144),
(00021, 151),
(00021, 152),
(00021, 153),
(00021, 154),
(00021, 161),
(00021, 162),
(00021, 163),
(00021, 164),
(00021, 171),
(00021, 172),
(00021, 173),
(00021, 174),
(00022, 011),
(00022, 012),
(00022, 013),
(00022, 021),
(00022, 022),
(00022, 023),
(00022, 024),
(00022, 031),
(00022, 032),
(00022, 033),
(00022, 041),
(00022, 042),
(00022, 043),
(00022, 044),
(00022, 051),
(00022, 052),
(00022, 053),
(00022, 061),
(00022, 062),
(00022, 063),
(00022, 064),
(00022, 071),
(00022, 072),
(00022, 073),
(00022, 081),
(00022, 082),
(00022, 083),
(00022, 084),
(00022, 091),
(00022, 092),
(00022, 093),
(00022, 094),
(00022, 113),
(00022, 114),
(00022, 121),
(00022, 122),
(00022, 123),
(00022, 124),
(00022, 131),
(00022, 132),
(00022, 133),
(00022, 134),
(00022, 141),
(00022, 142),
(00022, 143),
(00022, 144),
(00022, 151),
(00022, 152),
(00022, 153),
(00022, 154),
(00022, 161),
(00022, 162),
(00022, 163),
(00022, 164),
(00022, 171),
(00022, 172),
(00022, 173),
(00022, 174),
(00023, 011),
(00023, 012),
(00023, 013),
(00023, 021),
(00023, 022),
(00023, 023),
(00023, 024),
(00023, 031),
(00023, 032),
(00023, 033),
(00023, 041),
(00023, 042),
(00023, 043),
(00023, 044),
(00023, 051),
(00023, 052),
(00023, 053),
(00023, 061),
(00023, 062),
(00023, 063),
(00023, 064),
(00023, 071),
(00023, 072),
(00023, 073),
(00023, 081),
(00023, 082),
(00023, 083),
(00023, 084),
(00023, 091),
(00023, 092),
(00023, 093),
(00023, 094),
(00023, 101),
(00023, 102),
(00023, 103),
(00023, 104),
(00023, 111),
(00023, 112),
(00023, 113),
(00023, 114),
(00023, 121),
(00023, 122),
(00023, 123),
(00023, 124),
(00023, 131),
(00023, 132),
(00023, 133),
(00023, 134),
(00023, 141),
(00023, 142),
(00023, 143),
(00023, 144),
(00023, 171),
(00023, 172),
(00023, 173),
(00023, 174);

-- --------------------------------------------------------

--
-- Table structure for table `slot_time`
--

CREATE TABLE `slot_time` (
  `slot_time_id` int(3) UNSIGNED ZEROFILL NOT NULL,
  `day_of_week` char(3) NOT NULL CHECK (`day_of_week` in ('SUN','MON','TUE','WED','THU','FRI','SAT')),
  `time_start` time NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table `slot_time`
--

INSERT INTO `slot_time` (`slot_time_id`, `day_of_week`, `time_start`) VALUES
(011, 'MON', '12:00:00'),
(012, 'MON', '15:00:00'),
(013, 'MON', '18:00:00'),
(021, 'MON', '11:00:00'),
(022, 'MON', '14:00:00'),
(023, 'MON', '17:00:00'),
(024, 'MON', '20:00:00'),
(031, 'TUE', '12:00:00'),
(032, 'TUE', '15:00:00'),
(033, 'TUE', '18:00:00'),
(041, 'TUE', '11:00:00'),
(042, 'TUE', '14:00:00'),
(043, 'TUE', '17:00:00'),
(044, 'TUE', '20:00:00'),
(051, 'WED', '12:00:00'),
(052, 'WED', '15:00:00'),
(053, 'WED', '18:00:00'),
(061, 'WED', '11:00:00'),
(062, 'WED', '14:00:00'),
(063, 'WED', '17:00:00'),
(064, 'WED', '20:00:00'),
(071, 'THU', '12:00:00'),
(072, 'THU', '15:00:00'),
(073, 'THU', '18:00:00'),
(081, 'THU', '11:00:00'),
(082, 'THU', '14:00:00'),
(083, 'THU', '17:00:00'),
(084, 'THU', '20:00:00'),
(091, 'FRI', '12:00:00'),
(092, 'FRI', '15:00:00'),
(093, 'FRI', '18:00:00'),
(094, 'FRI', '21:00:00'),
(101, 'FRI', '11:00:00'),
(102, 'FRI', '14:00:00'),
(103, 'FRI', '17:00:00'),
(104, 'FRI', '20:00:00'),
(111, 'FRI', '13:00:00'),
(112, 'FRI', '16:00:00'),
(113, 'FRI', '19:00:00'),
(114, 'FRI', '22:00:00'),
(121, 'SAT', '12:00:00'),
(122, 'SAT', '15:00:00'),
(123, 'SAT', '18:00:00'),
(124, 'SAT', '21:00:00'),
(131, 'SAT', '11:00:00'),
(132, 'SAT', '14:00:00'),
(133, 'SAT', '17:00:00'),
(134, 'SAT', '20:00:00'),
(141, 'SAT', '13:00:00'),
(142, 'SAT', '16:00:00'),
(143, 'SAT', '19:00:00'),
(144, 'SAT', '22:00:00'),
(151, 'SUN', '12:00:00'),
(152, 'SUN', '15:00:00'),
(153, 'SUN', '18:00:00'),
(154, 'SUN', '21:00:00'),
(161, 'SUN', '11:00:00'),
(162, 'SUN', '14:00:00'),
(163, 'SUN', '17:00:00'),
(164, 'SUN', '20:00:00'),
(171, 'SUN', '13:00:00'),
(172, 'SUN', '16:00:00'),
(173, 'SUN', '19:00:00'),
(174, 'SUN', '22:00:00');

-- --------------------------------------------------------

--
-- Table structure for table `ticket`
--

CREATE TABLE `ticket` (
  `ticket_id` int(6) UNSIGNED ZEROFILL NOT NULL,
  `booking_date` date DEFAULT CURRENT_DATE() NOT NULL ,
  `total_price` decimal(5,2) NOT NULL,
  `pay_status` varchar(3) DEFAULT 'NO' NULL CHECK (`pay_status` in ('YES','NO')),
  `annoucement_id` int(5) UNSIGNED ZEROFILL NOT NULL,
  `slot_time_id` int(3) UNSIGNED ZEROFILL NOT NULL,
  `seat_id` varchar(3) NOT NULL,
  `hall_id` varchar(3) NOT NULL,
  `member_id` int(5) UNSIGNED ZEROFILL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table `ticket`
--

INSERT INTO `ticket` (`ticket_id`,`booking_date`, `total_price`, `pay_status`, `annoucement_id`, `slot_time_id`, `seat_id`, `hall_id`, `member_id`) VALUES
(000001, '2000-01-01', '120.00', 'YES', 00001, 121, 'A05', 'T1', 00003),
(000002, '2000-01-01', '300.00', 'YES', 00001, 121, 'B06', 'T1', 00001),
(000003, '2000-01-10', '400.00', 'YES', 00001, 043, 'C04', 'T3', 00002),
(000004, '2000-01-05', '400.00', 'YES', 00001, 131, 'C05', 'T4', 00004),
(000005, '2000-01-01', '400.00', 'YES', 00001, 042, 'C02', 'T5', NULL),
(000006, '2000-01-15', '120.00', 'YES', 00001, 042, 'A05', 'T4', 00003),
(000007, '2000-01-17', '300.00', 'YES', 00002, 031, 'B08', 'T2', 00002),
(000008, '2000-01-20', '120.00', 'YES', 00002, 031, 'A09', 'T1', NULL),
(000009, '2000-01-15', '120.00', 'YES', 00002, 132, 'A05', 'T3', 00005),
(000010, '2000-01-20', '120.00', 'YES', 00002, 141, 'A05', 'T5', 00004),
(000011, '2000-02-03', '300.00', 'YES', 00003, 171, 'B05', 'T5', 00003),
(000012, '2000-02-03', '120.00', 'YES', 00003, 141, 'A08', 'T1', 00001),
(000013, '2000-02-12', '120.00', 'YES', 00004, 171, 'A06', 'T5', NULL),
(000014, '2000-02-16', '120.00', 'YES', 00004, 162, 'A05', 'T4', NULL),
(000015, '2000-02-15', '400.00', 'YES', 00004, 144, 'C06', 'T2', 00004),
(000016, '2000-02-26', '120.00', 'YES', 00005, 024, 'A06', 'T5', NULL),
(000017, '2000-02-26', '120.00', 'YES', 00005, 012, 'A05', 'T2', NULL),
(000018, '2000-03-11', '400.00', 'YES', 00006, 023, 'C04', 'T4', 00004),
(000019, '2000-03-11', '300.00', 'YES', 00006, 022, 'A05', 'T5', 00002),
(000020, '2000-03-11', '300.00', 'YES', 00006, 012, 'B06', 'T1', NULL);

-- --------------------------------------------------------

--
-- Table structure for table `tier_seat`
--

CREATE TABLE `tier_seat` (
  `tier_seat_id` varchar(2) NOT NULL,
  `tier_name` varchar(10) NOT NULL DEFAULT 'Normal',
  `tier_price` decimal(5,2) NOT NULL DEFAULT 200.00
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table `tier_seat`
--

INSERT INTO `tier_seat` (`tier_seat_id`, `tier_name`, `tier_price`) VALUES
('1', 'Normal', '120.00'),
('2', 'Honeymoon', '300.00'),
('3', 'Premium', '400.00');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `hall`
--
ALTER TABLE `hall`
  ADD PRIMARY KEY (`hall_id`);

--
-- Indexes for table `member`
--
ALTER TABLE `member`
  ADD PRIMARY KEY (`member_id`),
  ADD UNIQUE KEY `member_name` (`member_name`),
  ADD UNIQUE KEY `email` (`email`),
  ADD UNIQUE KEY `phone` (`phone`);

ALTER TABLE `member` CHANGE `member_id` `member_id` int(5) UNSIGNED ZEROFILL NOT NULL AUTO_INCREMENT;
--
-- Indexes for table `movie`
--
ALTER TABLE `movie`
  ADD PRIMARY KEY (`movie_id`);
ALTER TABLE `movie` CHANGE `movie_id` `movie_id` int(5) UNSIGNED ZEROFILL NOT NULL AUTO_INCREMENT;
--
-- Indexes for table `seat`
--
ALTER TABLE `seat`
  ADD PRIMARY KEY (`seat_id`,`hall_id`),
  ADD KEY `hall_id` (`hall_id`),
  ADD KEY `tier_seat_id` (`tier_seat_id`);

--
-- Indexes for table `show_schedule`
--
ALTER TABLE `show_annoucement`
  ADD PRIMARY KEY (`annoucement_id`),
  ADD KEY `movie_id` (`movie_id`);

--
-- Indexes for table `show_time`
--
ALTER TABLE `show_time`
  ADD PRIMARY KEY (`annoucement_id`,`slot_time_id`),
  ADD KEY `slot_time_id` (`slot_time_id`);

--
-- Indexes for table `slot_time`
--
ALTER TABLE `slot_time`
  ADD PRIMARY KEY (`slot_time_id`);

--
-- Indexes for table `ticket`
--
ALTER TABLE `ticket`
  ADD PRIMARY KEY (`ticket_id`),
  ADD KEY `annoucement_id` (`annoucement_id`),
  ADD KEY `slot_time_id` (`slot_time_id`),
  ADD KEY `seat_id` (`seat_id`),
  ADD KEY `hall_id` (`hall_id`),
  ADD KEY `member_id` (`member_id`);

ALTER TABLE `ticket` CHANGE `ticket_id` `ticket_id` int(6) UNSIGNED ZEROFILL NOT NULL AUTO_INCREMENT;
--
-- Indexes for table `tier_seat`
--
ALTER TABLE `tier_seat`
  ADD PRIMARY KEY (`tier_seat_id`);

--
-- Constraints for dumped tables
--

--
-- Constraints for table `seat`
--
ALTER TABLE `seat`
  ADD CONSTRAINT `seat_ibfk_1` FOREIGN KEY (`hall_id`) REFERENCES `hall` (`hall_id`),
  ADD CONSTRAINT `seat_ibfk_2` FOREIGN KEY (`tier_seat_id`) REFERENCES `tier_seat` (`tier_seat_id`);

--
-- Constraints for table `show_schedule`
--
ALTER TABLE `show_annoucement`
  ADD CONSTRAINT `show_annoucement_ibfk_1` FOREIGN KEY (`movie_id`) REFERENCES `movie` (`movie_id`);

--
-- Constraints for table `show_time`
--
ALTER TABLE `show_time`
  ADD CONSTRAINT `show_time_ibfk_1` FOREIGN KEY (`annoucement_id`) REFERENCES `show_annoucement` (`annoucement_id`),
  ADD CONSTRAINT `show_time_ibfk_2` FOREIGN KEY (`slot_time_id`) REFERENCES `slot_time` (`slot_time_id`);

--
-- Constraints for table `ticket`
--
ALTER TABLE `ticket`
  ADD CONSTRAINT `ticket_ibfk_1` FOREIGN KEY (`annoucement_id`) REFERENCES `show_annoucement` (`annoucement_id`),
  ADD CONSTRAINT `ticket_ibfk_2` FOREIGN KEY (`slot_time_id`) REFERENCES `slot_time` (`slot_time_id`),
  ADD CONSTRAINT `ticket_ibfk_3` FOREIGN KEY (`seat_id`) REFERENCES `seat` (`seat_id`),
  ADD CONSTRAINT `ticket_ibfk_4` FOREIGN KEY (`hall_id`) REFERENCES `seat` (`hall_id`),
  ADD CONSTRAINT `ticket_ibfk_5` FOREIGN KEY (`member_id`) REFERENCES `member` (`member_id`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
