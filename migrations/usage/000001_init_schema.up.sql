

CREATE TABLE usage (
    region text,
    account_id text,
    ts BIGINT,
    counter int,
	ts_history BIGINT[],
    PRIMARY KEY(region, account_id)
);

-- 1. Create the trigger function
CREATE OR REPLACE FUNCTION trim_array_elements()
RETURNS TRIGGER AS $$
DECLARE
    trimmed_array BIGINT[];
    element BIGINT;
BEGIN
    -- Check if the column exists in the NEW record (for INSERT/UPDATE)
    IF array_length(NEW.ts_history, 1) > 3 THEN
        -- Iterate through the array elements and trim each one
        trimmed_array := NEW.ts_history[2:];
        NEW.ts_history := trimmed_array;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Create the trigger
CREATE TRIGGER trg_trim_array_elements
BEFORE INSERT OR UPDATE OF ts_history ON usage
FOR EACH ROW
EXECUTE FUNCTION trim_array_elements();
