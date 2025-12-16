import os
from typing import Any
from dotenv import load_dotenv
import asyncio
from pydantic import BaseModel, Field
from restack_ai.function import NonRetryableError, function, log
from brightdata import bdclient

load_dotenv()


class GetProfilePostsInput(BaseModel):
    """Input parameters for getting a LinkedIn profile's posts."""

    model_config = {
        "strict": True,
        "extra": "forbid",
        "validate_assignment": True,
        "str_strip_whitespace": True,
    }

    profile_url: str = Field(
        ...,
        title="LinkedIn Profile URL",
        description="The URL of the LinkedIn profile's post section.",
        example="https://www.linkedin.com/in/williamhgates/recent-activity/all/",
    )


def raise_exception(message: str) -> None:
    log.error("get_linkedin_profile_posts_brightdata function failed", error=message)
    raise NonRetryableError(message)


@function.defn()
async def trigger_linkedin_profile_posts_scrape(function_input: GetProfilePostsInput) -> dict[str, Any]:
    """Trigger a LinkedIn profile posts scrape and return the snapshot_id."""
    try:
        api_token = os.environ.get("BRIGHT_DATA_API_TOKEN")
        if not api_token:
            raise_exception("BRIGHT_DATA_API_TOKEN is not set")

        bd = bdclient(api_token)
        
        profile_url = function_input.profile_url.split('recent-activity')[0]
        log.info(f"Initiating post discovery for profile {profile_url}")

        initial_response = await asyncio.to_thread(
            bd.search_linkedin.posts, profile_url=profile_url
        )

        snapshot_id = initial_response.get("snapshot_id")
        if not snapshot_id:
            status = initial_response.get("status")
            # If status is "starting" without snapshot_id, that's an error condition
            if status == "starting":
                raise_exception(f"Received 'starting' status but no snapshot_id found in response: {initial_response}")
            # Otherwise, assume we got the data directly (shouldn't happen, but handle it)
            log.info("Received synchronous response for posts from Bright Data.")
            return initial_response

        log.info(f"Post discovery initiated. Snapshot ID: {snapshot_id}")
        return {"snapshot_id": snapshot_id}

    except Exception as e:
        error_message = f"trigger_linkedin_profile_posts_scrape failed: {e}"
        raise NonRetryableError(error_message) from e


@function.defn()
async def get_linkedin_profile_posts_brightdata(function_input: GetProfilePostsInput) -> Any:
    """Legacy function - kept for backward compatibility. Use trigger_linkedin_profile_posts_scrape + download_brightdata_snapshot instead."""
    try:
        api_token = os.environ.get("BRIGHT_DATA_API_TOKEN")
        if not api_token:
            raise_exception("BRIGHT_DATA_API_TOKEN is not set")

        bd = bdclient(api_token)
        
        profile_url = function_input.profile_url.split('recent-activity')[0]
        log.info(f"Initiating post discovery for profile {profile_url}")

        initial_response = await asyncio.to_thread(
            bd.search_linkedin.posts, profile_url=profile_url
        )

        snapshot_id = initial_response.get("snapshot_id")
        if not snapshot_id:
            log.info("Received synchronous response for posts from Bright Data.")
            return initial_response

        log.info(f"Post discovery initiated. Snapshot ID: {snapshot_id}")

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
        posts_data = await asyncio.to_thread(bd.download_snapshot, snapshot_id=snapshot_id)

        if not posts_data:
            raise_exception("Failed to download posts data from Bright Data snapshot.")

    except Exception as e:
        error_message = f"get_linkedin_profile_posts_brightdata failed: {e}"
        raise NonRetryableError(error_message) from e
    else:
        log.info(f"Successfully discovered posts for {profile_url}")
        return posts_data
