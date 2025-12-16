import os
from typing import Any
from dotenv import load_dotenv
import asyncio
from pydantic import BaseModel, Field
from restack_ai.function import NonRetryableError, function, log
from brightdata import bdclient

load_dotenv()

# Changes to this file should also be reflected in the Phantombuster version


class GetProfileInput(BaseModel):
    """Input parameters for getting a LinkedIn profile."""

    model_config = {
        "strict": True,
        "extra": "forbid",
        "validate_assignment": True,
        "str_strip_whitespace": True,
    }

    profile_url: str = Field(
        ...,
        title="LinkedIn Profile URL",
        description="The URL of the LinkedIn profile.",
        example="https://www.linkedin.com/in/williamhgates/",
    )


class SnapshotIdInput(BaseModel):
    """Input parameters for downloading a Bright Data snapshot."""

    model_config = {
        "strict": True,
        "extra": "forbid",
        "validate_assignment": True,
        "str_strip_whitespace": True,
    }

    snapshot_id: str = Field(
        ...,
        title="Snapshot ID",
        description="The snapshot ID returned by Bright Data.",
    )


def raise_exception(message: str) -> None:
    log.error("get_linkedin_profile_brightdata function failed", error=message)
    raise NonRetryableError(message)


@function.defn()
async def trigger_linkedin_profile_scrape(function_input: GetProfileInput) -> dict[str, Any]:
    """Trigger a LinkedIn profile scrape and return the snapshot_id."""
    try:
        api_token = os.environ.get("BRIGHT_DATA_API_TOKEN")
        if not api_token:
            raise_exception("BRIGHT_DATA_API_TOKEN is not set")

        bd = bdclient(api_token)
        log.info(f"Initiating scrape for {function_input.profile_url}")

        # Use sync=False to get snapshot_id immediately
        initial_response = await asyncio.to_thread(
            bd.scrape_linkedin.profiles, function_input.profile_url, sync=False
        )
        
        log.info(f"Initial response from Bright Data: {initial_response}")

        # Extract snapshot_id from the response
        snapshot_id = initial_response.get("snapshot_id")
        
        # Check if we got a direct result (no snapshot_id means it completed synchronously)
        if not snapshot_id:
            status = initial_response.get("status")
            # If status is "starting" without snapshot_id, that's an error condition
            if status == "starting":
                raise_exception(f"Received 'starting' status but no snapshot_id found in response: {initial_response}")
            # Otherwise, assume we got the data directly (shouldn't happen with sync=False, but handle it)
            log.info("Received synchronous response for profile from Bright Data.")
            return initial_response

        log.info(f"Scrape initiated. Snapshot ID: {snapshot_id}")
        return {"snapshot_id": snapshot_id}

    except Exception as e:
        error_message = f"trigger_linkedin_profile_scrape failed: {e}"
        raise NonRetryableError(error_message) from e


@function.defn()
async def download_brightdata_snapshot(function_input: SnapshotIdInput) -> Any:
    """Download a Bright Data snapshot. Raises RetryableError if snapshot is not ready yet.
    
    Returns:
        - list: When snapshot is ready, returns the list of records
        - dict: When snapshot is still processing, returns status dict (will raise RetryableError)
    """
    from restack_ai.function import RetryableError
    
    try:
        api_token = os.environ.get("BRIGHT_DATA_API_TOKEN")
        if not api_token:
            raise_exception("BRIGHT_DATA_API_TOKEN is not set")

        bd = bdclient(api_token)
        snapshot_id = function_input.snapshot_id
        
        log.info(f"Downloading snapshot {snapshot_id}...")
        snapshot_data = await asyncio.to_thread(bd.download_snapshot, snapshot_id=snapshot_id)
        
        # Bright Data returns a list when the snapshot is ready (the actual data)
        # or a dict with status when it's still processing
        if isinstance(snapshot_data, list):
            log.info(f"Snapshot {snapshot_id} is ready. Retrieved {len(snapshot_data)} record(s).")
            return snapshot_data
        
        # If it's a dict, check the status
        if isinstance(snapshot_data, dict):
            status = snapshot_data.get("status")
            log.info(f"Snapshot {snapshot_id} status: {status}")

            if status == "done":
                log.info(f"Snapshot {snapshot_id} is ready.")
                return snapshot_data
            elif status == "failed":
                raise_exception(f"Bright Data snapshot {snapshot_id} failed. Details: {snapshot_data}")
            else:
                # Status is "starting", "not_ready", or similar - not ready yet, retry
                log.info(f"Snapshot {snapshot_id} is not ready yet (status: {status}), will retry...")
                raise RetryableError(f"Snapshot {snapshot_id} is not ready yet. Status: {status}")
        
        # Unexpected response type
        raise_exception(f"Unexpected response type from Bright Data snapshot {snapshot_id}: {type(snapshot_data)}")

    except RetryableError:
        # Re-raise retryable errors as-is
        raise
    except Exception as e:
        error_message = f"download_brightdata_snapshot failed: {e}"
        raise NonRetryableError(error_message) from e


@function.defn()
async def get_linkedin_profile_brightdata(function_input: GetProfileInput) -> Any:
    """Legacy function - kept for backward compatibility. Use trigger_linkedin_profile_scrape + download_brightdata_snapshot instead."""
    try:
        api_token = os.environ.get("BRIGHT_DATA_API_TOKEN")
        if not api_token:
            raise_exception("BRIGHT_DATA_API_TOKEN is not set")

        bd = bdclient(api_token)
        log.info(f"Initiating scrape for {function_input.profile_url}")

        # Use sync=False to get snapshot_id immediately, then poll for completion
        initial_response = await asyncio.to_thread(
            bd.scrape_linkedin.profiles, function_input.profile_url, sync=False
        )
        
        log.info(f"Initial response from Bright Data: {initial_response}")

        # Extract snapshot_id from the response
        snapshot_id = initial_response.get("snapshot_id")
        
        # Check if we got a direct result (no snapshot_id means it completed synchronously)
        if not snapshot_id:
            status = initial_response.get("status")
            # If status is "starting" without snapshot_id, that's an error condition
            if status == "starting":
                raise_exception(f"Received 'starting' status but no snapshot_id found in response: {initial_response}")
            # Otherwise, assume we got the data directly
            log.info("Received synchronous response for profile from Bright Data.")
            return initial_response

        log.info(f"Scrape initiated. Snapshot ID: {snapshot_id}")

        # Poll for completion
        max_attempts = 60  # 5 minutes max (60 * 5 seconds)
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            log.info(f"Checking status for snapshot {snapshot_id} (attempt {attempt}/{max_attempts})...")
            status_response = await asyncio.to_thread(bd.download_snapshot, snapshot_id=snapshot_id)
            
            # Bright Data returns a list when ready, or a dict with status when processing
            if isinstance(status_response, list):
                log.info(f"Snapshot {snapshot_id} is ready. Retrieved {len(status_response)} record(s).")
                return status_response
            
            if isinstance(status_response, dict):
                status = status_response.get("status")
                log.info(f"Snapshot status: {status}")

                if status == "done":
                    break
                elif status == "failed":
                    raise_exception(f"Bright Data snapshot {snapshot_id} failed. Details: {status_response}")
            
            await asyncio.sleep(5)
        
        if attempt >= max_attempts:
            raise_exception(f"Timeout waiting for Bright Data snapshot {snapshot_id} to complete after {max_attempts} attempts")
        
        log.info(f"Snapshot {snapshot_id} is ready. Downloading result.")
        profile_data = await asyncio.to_thread(bd.download_snapshot, snapshot_id=snapshot_id)

        if not profile_data:
            raise_exception("Failed to download profile data from Bright Data snapshot.")

    except Exception as e:
        error_message = f"get_linkedin_profile_brightdata failed: {e}"
        raise NonRetryableError(error_message) from e
    else:
        log.info(f"Successfully scraped profile for {function_input.profile_url}")
        return profile_data
