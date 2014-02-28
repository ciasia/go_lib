
-- CREATE TABLE client  (`id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, `name` VARCHAR(1000) NULL, `notes` TEXT NULL, PRIMARY KEY (`id`));

INSERT INTO client (name) SELECT DISTINCT(company) FROM project WHERE company IS NOT NULL AND company != "";

UPDATE project  LEFT JOIN client ON client.name = project.company SET project.client = client.id;

UPDATE project SET currency = "AUD" WHERE currency IS NULL or currency = "";
UPDATE project SET currency = "NZD" WHERE currency = "NZ$";
UPDATE project SET currency = "USD" WHERE currency = "US$";

UPDATE project SET gst = 1 WHERE currency = "AUD";
UPDATE project SET gst = 0 WHERE currency != "AUD";

ALTER TABLE project ADD currency_dd INT(2);

UPDATE project SET currency_dd = 10 WHERE currency = "AUD";
UPDATE project SET currency_dd = 20 WHERE currency = "USD";
UPDATE project SET currency_dd = 30 WHERE currency = "NZD";
UPDATE project SET currency_dd = 40 WHERE currency = "INR";
UPDATE project SET currency_dd = 50 WHERE currency = "SGD";
UPDATE project SET currency_dd = 60 WHERE currency = "AED";

ALTER TABLE project DROP currency; 
ALTER TABLE project CHANGE currency_dd currency INT(2);
UPDATE invoice SET status = 20 WHERE status IS NULL;
UPDATE invoice SET status = 40 WHERE invoice_date > (2014-01-01); 
UPDATE invoice SET status = 30 WHERE invoice_date > (2014-02-01); 
