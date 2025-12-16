from typing import Any
from pydantic import BaseModel, Field
from restack_ai.function import NonRetryableError, function, log


class GetReactionsInput(BaseModel):
    """Input parameters for getting a LinkedIn profile's reactions."""

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


def raise_exception(message: str) -> None:
    log.error("get_linkedin_profile_reactions_brightdata function failed", error=message)
    raise NonRetryableError(message)


@function.defn()
async def get_linkedin_profile_reactions_brightdata(function_input: GetReactionsInput) -> Any:
    """
    NOTE: Scraping reactions is a complex, multi-step task (get posts, then get
    reactions for each post) that requires a more advanced scraping solution.
    This functionality is not implemented in this version.
    """
    log.warn("get_linkedin_profile_reactions_brightdata is not implemented.")
    raise NonRetryableError(
        "Scraping reactions from a LinkedIn profile is a premium feature "
        "and is not yet supported in this implementation."
    )
