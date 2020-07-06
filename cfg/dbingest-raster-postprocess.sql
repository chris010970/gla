BEGIN;
ALTER TABLE !SCHEMA!.!TEMP_TABLE! ADD COLUMN fid INT;
UPDATE !SCHEMA!.!TEMP_TABLE! rt SET fid=newid
    FROM ( SELECT filename, nextval( 'fid_!SCHEMA!_!PRODUCT!_seq' ) newid
    FROM !SCHEMA!.!TEMP_TABLE!
    GROUP BY filename) foo
    WHERE rt.filename = foo.filename;

ALTER TABLE !SCHEMA!.!TEMP_TABLE! ADD COLUMN fdate TIMESTAMP;
UPDATE !SCHEMA!.!TEMP_TABLE! rt SET fdate='!TIMESTAMP!'
    FROM ( SELECT filename 
    FROM !SCHEMA!.!TEMP_TABLE!
    GROUP BY filename) foo
    WHERE rt.filename = foo.filename;

INSERT INTO !SCHEMA!.!PRODUCT! (rid,fid,fdate,rast) SELECT rid,fid,fdate,rast FROM !SCHEMA!.!TEMP_TABLE!;

WITH vals ( name, description, keywords ) AS ( VALUES !PRODUCT_DATA! ) SELECT * INTO TEMPORARY TABLE product_temp FROM vals;
INSERT INTO !SCHEMA!.product ( name, description, keywords )
    SELECT name, description, keywords FROM product_temp ON CONFLICT DO NOTHING;

WITH vals ( name, description, keywords, units ) AS ( VALUES !MEASUREMENT_DATA! ) SELECT * INTO TEMPORARY TABLE measurement_temp FROM vals;
INSERT INTO !SCHEMA!.measurement ( name, description, keywords, units, pid ) 
    SELECT name, description, keywords, units, ( SELECT id FROM !SCHEMA!.product WHERE name = '!PRODUCT!' ) pid FROM measurement_temp ON CONFLICT DO NOTHING;

INSERT INTO !SCHEMA!.cat ( fdate, fid, pid, pathname, hull ) VALUES 
( '!TIMESTAMP!', 
  ( SELECT currval( 'fid_!SCHEMA!_!PRODUCT!_seq' ) ),
  ( SELECT id FROM !SCHEMA!.product WHERE name = '!PRODUCT!' ),
  '!PATHNAME!',
  NULL
);

DROP TABLE !SCHEMA!.!TEMP_TABLE!;

END;
