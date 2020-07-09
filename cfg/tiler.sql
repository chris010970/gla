CREATE OR REPLACE FUNCTION Tiler(size float, geom Geometry) RETURNS
SETOF geometry AS
$$
DECLARE
xlength float;
ylength float;
midpoint float;
splitline Geometry;
splitpoly Geometry;
g Geometry;
s Geometry;
i int;
BEGIN
IF ST_IsCollection(geom) THEN
    FOR i IN SELECT generate_series(1, ST_NumGeometries(geom)) LOOP
        FOR s IN SELECT Tiler(size, ST_GeometryN(geom, i)) LOOP
            RETURN NEXT s;
        END LOOP;
    END LOOP;
ELSE
    xlength := ST_XMax(geom) - ST_XMin(geom);
    ylength := ST_YMax(geom) - ST_YMin(geom);

    IF xlength > size THEN
        midpoint = ST_XMin(geom) + 0.5*xlength;
        splitline = ST_MakeLine(ST_MakePoint(midpoint, -1e10),
                                ST_MakePoint(midpoint, 1e10));
    ELSE
        IF ylength > size THEN
            midpoint = ST_YMin(geom) + 0.5*ylength;
            splitline = ST_MakeLine(ST_MakePoint( -1e10, midpoint),
                                    ST_MakePoint( 1e10,  midpoint));
        END IF;
    END IF;

    IF xlength > size OR ylength > size THEN
        splitline := ST_SetSRID(splitline, ST_SRID(geom));
        splitpoly = ST_Split(geom, splitline);
        FOR g IN SELECT (ST_Dump(splitpoly)).geom LOOP
            FOR s IN SELECT Tiler(size, g) LOOP
                RETURN NEXT s;
            END LOOP;
        END LOOP ;
    ELSE
        RETURN NEXT geom;
    END IF;
END IF;
RETURN;
END;
$$
language plpgsql;
