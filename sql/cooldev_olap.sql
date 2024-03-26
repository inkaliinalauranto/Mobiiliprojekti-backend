-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema cooldev_olap
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema cooldev_olap
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `cooldev_olap` DEFAULT CHARACTER SET utf8 ;
USE `cooldev_olap` ;

-- -----------------------------------------------------
-- Table `cooldev_olap`.`sensors_dim`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `cooldev_olap`.`sensors_dim` (
  `sensor_key` INT NOT NULL AUTO_INCREMENT,
  `sensor_id` VARCHAR(100) NOT NULL,
  `sensor_name` VARCHAR(225) NOT NULL,
  `device_id` VARCHAR(100) NOT NULL,
  `device_name` VARCHAR(255) NOT NULL,
  `unit` VARCHAR(10) NOT NULL,
  PRIMARY KEY (`sensor_key`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `cooldev_olap`.`dates_dim`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `cooldev_olap`.`dates_dim` (
  `date_key` INT NOT NULL AUTO_INCREMENT,
  `year` INT NOT NULL,
  `month` INT NOT NULL,
  `week` INT NOT NULL,
  `day` INT NOT NULL,
  `hour` INT NOT NULL,
  `min` INT NOT NULL,
  `sec` INT NOT NULL,
  `ms` INT NOT NULL,
  PRIMARY KEY (`date_key`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `cooldev_olap`.`measurements_fact`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `cooldev_olap`.`measurements_fact` (
  `sensor_key` INT NOT NULL,
  `date_key` INT NOT NULL,
  `value` FLOAT NOT NULL,
  INDEX `fk_measurements_fact_sensors_dim_idx` (`sensor_key` ASC),
  PRIMARY KEY (`sensor_key`, `date_key`),
  INDEX `fk_measurements_fact_dates_dim1_idx` (`date_key` ASC),
  CONSTRAINT `fk_measurements_fact_sensors_dim`
    FOREIGN KEY (`sensor_key`)
    REFERENCES `cooldev_olap`.`sensors_dim` (`sensor_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_measurements_fact_dates_dim1`
    FOREIGN KEY (`date_key`)
    REFERENCES `cooldev_olap`.`dates_dim` (`date_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `cooldev_olap`.`auth_roles`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `cooldev_olap`.`auth_roles` (
  `role_id` INT NOT NULL AUTO_INCREMENT,
  `role_name` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`role_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `cooldev_olap`.`auth_users`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `cooldev_olap`.`auth_users` (
  `user_id` INT NOT NULL AUTO_INCREMENT,
  `created_at` DATETIME NOT NULL,
  `deleted_at` DATETIME NULL,
  `username` VARCHAR(255) NOT NULL,
  `password` VARCHAR(255) NOT NULL,
  `access_jti` VARCHAR(512) NOT NULL,
  `auth_role_id` INT NOT NULL,
  PRIMARY KEY (`user_id`),
  INDEX `fk_auth_users_auth_roles1_idx` (`auth_role_id` ASC),
  CONSTRAINT `fk_auth_users_auth_roles1`
    FOREIGN KEY (`auth_role_id`)
    REFERENCES `cooldev_olap`.`auth_roles` (`role_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
