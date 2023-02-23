from dotenv import load_dotenv
from os import environ
from os.path import join, dirname
from pydantic import BaseModel, Field
from fastapi_cloudauth.cognito import Cognito, CognitoCurrentUser, CognitoClaims
from fastapi import Depends

load_dotenv(join(dirname(__file__), "/api/ssParser/.env"))

auth = Cognito(
    region = environ.get("AWS_DEFAULT_REGION"),
    userPoolId = environ.get("USERPOOLID"),
    client_id = environ.get("APPCLIENTID")
)

"""
    This API uses scoped endpoint protection based on JWT access tokens from the
    AWS Cognito User Pools maintained by our sponsor.

    TO ADD SCOPED PROTECTION TO AN ENDPOINT

    import the following:

    from fastapi_cloudauth.verification import Operator
    from fastapi import Depends
    from .cognito_auth import auth

    in function parameters, add:

    dependencies = Depends(auth.scope(["Admin", "Moderator"], op=Operator._any))

    ex.

    def function(<parameters>, dependencies = Depends(auth.scope(["Admin", "Moderator"], op=Operator._any))):
        <function body>

    This will automatically validate the user's access token and will
    protect the endpoint from anyone not logged in as a member of the Admin or Moderator
    groups in our AWS Cognito User Pool. Newly-created accounts must be added to these groups
    through the AWS website manually.

    The "op=Operator._any" part indicates that the user is authorized for EITHER scope.
    Removing this operator will limit the endpoint to users who are part of both the Admin
    group AND the Moderator group; at this time, no such overlap exists nor is it necessary.

    To limit an endpoint to, for example, just admins, change instead to:

    dependencies = Depends(auth.scope(["Admin"]))

    or vice versa for the desired role.
    This role MUST match the exact spelling and capitalization of the group as listed
    on the AWS website, as this is how it will be listed in the user's access token.

    An endpoint protected in this scope can return a number of different error messages related
    to its protection.
    401 Unauthorized - the user is not logged in and has not provided an access token.
    403 Forbidden - the user does not have the proper scope.
"""