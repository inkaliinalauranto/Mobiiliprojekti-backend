-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema cooldev_oltp
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema cooldev_oltp
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `cooldev_oltp` DEFAULT CHARACTER SET utf8 ;
USE `cooldev_oltp` ;

-- -----------------------------------------------------
-- Table `cooldev_oltp`.`auth_roles`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `cooldev_oltp`.`auth_roles` (
  `role_id` INT NOT NULL AUTO_INCREMENT,
  `role_name` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`role_id`),
  UNIQUE INDEX `role_name_UNIQUE` (`role_name` ASC))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `cooldev_oltp`.`auth_users`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `cooldev_oltp`.`auth_users` (
  `user_id` INT NOT NULL AUTO_INCREMENT,
  `created_at` DATETIME NOT NULL,
  `deleted_at` DATETIME NULL,
  `username` VARCHAR(255) NOT NULL,
  `password` VARCHAR(255) NOT NULL,
  `access_jti` VARCHAR(512) NULL,
  `auth_role_id` INT NOT NULL,
  PRIMARY KEY (`user_id`),
  INDEX `fk_auth_users_auth_roles1_idx` (`auth_role_id` ASC),
  UNIQUE INDEX `username_UNIQUE` (`username` ASC),
  CONSTRAINT `fk_auth_users_auth_roles1`
    FOREIGN KEY (`auth_role_id`)
    REFERENCES `cooldev_oltp`.`auth_roles` (`role_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;


INSERT INTO `auth_roles` (`role_id`, `role_name`) VALUES (NULL, 'user');