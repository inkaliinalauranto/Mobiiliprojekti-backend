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
  `value` FLOAT(10,3) NOT NULL,
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
-- Table `cooldev_olap`.`total_consumptions_fact`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `cooldev_olap`.`total_consumptions_fact` (
  `sensor_key` INT NOT NULL,
  `date_key` INT NOT NULL,
  `value` FLOAT(10,3) NOT NULL,
  INDEX `fk_total_consumptions_fact_sensors_dim_idx` (`sensor_key` ASC),
  PRIMARY KEY (`sensor_key`, `date_key`),
  INDEX `fk_total_consumptions_fact_dates_dim1_idx` (`date_key` ASC),
  CONSTRAINT `fk_total_consumptions_fact_sensors_dim1`
    FOREIGN KEY (`sensor_key`)
    REFERENCES `cooldev_olap`.`sensors_dim` (`sensor_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_total_consumptions_fact_dates_dim1`
    FOREIGN KEY (`date_key`)
    REFERENCES `cooldev_olap`.`dates_dim` (`date_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `cooldev_olap`.`productions_fact`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `cooldev_olap`.`productions_fact` (
  `sensor_key` INT NOT NULL,
  `date_key` INT NOT NULL,
  `value` FLOAT(10,3) NOT NULL,
  INDEX `fk_productions__fact_sensors_dim_idx` (`sensor_key` ASC),
  PRIMARY KEY (`sensor_key`, `date_key`),
  INDEX `fk_productions_fact_dates_dim1_idx` (`date_key` ASC),
  CONSTRAINT `fk_productions_fact_sensors_dim1`
    FOREIGN KEY (`sensor_key`)
    REFERENCES `cooldev_olap`.`sensors_dim` (`sensor_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_productions_fact_dates_dim1`
    FOREIGN KEY (`date_key`)
    REFERENCES `cooldev_olap`.`dates_dim` (`date_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `cooldev_olap`.`heating_consumptions_fact`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `cooldev_olap`.`heating_consumptions_fact` (
  `sensor_key` INT NOT NULL,
  `date_key` INT NOT NULL,
  `value` FLOAT(10,3) NOT NULL,
  INDEX `fk_heating_consumptions_fact_sensors_dim_idx` (`sensor_key` ASC),
  PRIMARY KEY (`sensor_key`, `date_key`),
  INDEX `fk_heating_consumptions_fact_dates_dim1_idx` (`date_key` ASC),
  CONSTRAINT `fk_heating_consumptions_fact_sensors_dim1`
    FOREIGN KEY (`sensor_key`)
    REFERENCES `cooldev_olap`.`sensors_dim` (`sensor_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_heating_consumptions_fact_dates_dim1`
    FOREIGN KEY (`date_key`)
    REFERENCES `cooldev_olap`.`dates_dim` (`date_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `cooldev_olap`.`lighting_consumptions_fact`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `cooldev_olap`.`lighting_consumptions_fact` (
  `sensor_key` INT NOT NULL,
  `date_key` INT NOT NULL,
  `value` FLOAT(10,3) NOT NULL,
  INDEX `fk_lights_consumptions_fact_sensors_dim_idx` (`sensor_key` ASC),
  PRIMARY KEY (`sensor_key`, `date_key`),
  INDEX `fk_lights_consumptions_fact_dates_dim1_idx` (`date_key` ASC),
  CONSTRAINT `fk_lights_consumptions_fact_sensors_dim1`
    FOREIGN KEY (`sensor_key`)
    REFERENCES `cooldev_olap`.`sensors_dim` (`sensor_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_lights_consumptions_fact_dates_dim1`
    FOREIGN KEY (`date_key`)
    REFERENCES `cooldev_olap`.`dates_dim` (`date_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `cooldev_olap`.`outlets_consumptions_fact`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `cooldev_olap`.`outlets_consumptions_fact` (
  `sensor_key` INT NOT NULL,
  `date_key` INT NOT NULL,
  `value` FLOAT(10,3) NOT NULL,
  INDEX `fk_outlets_consumption_fact_sensors_dim_idx` (`sensor_key` ASC),
  PRIMARY KEY (`sensor_key`, `date_key`),
  INDEX `fk_outlets_consumption_fact_dates_dim1_idx` (`date_key` ASC),
  CONSTRAINT `fk_outlets_consumption_fact_sensors_dim1`
    FOREIGN KEY (`sensor_key`)
    REFERENCES `cooldev_olap`.`sensors_dim` (`sensor_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_outlets_consumption_fact_dates_dim1`
    FOREIGN KEY (`date_key`)
    REFERENCES `cooldev_olap`.`dates_dim` (`date_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;



-- -----------------------------------------------------
-- Table `cooldev_olap`.`temperatures_fact`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `cooldev_olap`.`temperatures_fact` (
  `sensor_key` INT NOT NULL,
  `date_key` INT NOT NULL,
  `value` FLOAT(10,3) NOT NULL,
  INDEX `fk_temperatures_fact_sensors_dim_idx` (`sensor_key` ASC),
  PRIMARY KEY (`sensor_key`, `date_key`),
  INDEX `fk_temperatures_fact_dates_dim1_idx` (`date_key` ASC),
  CONSTRAINT `fk_temperatures_sensors_dim1`
    FOREIGN KEY (`sensor_key`)
    REFERENCES `cooldev_olap`.`sensors_dim` (`sensor_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_temperatures_fact_dates_dim1`
    FOREIGN KEY (`date_key`)
    REFERENCES `cooldev_olap`.`dates_dim` (`date_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;


