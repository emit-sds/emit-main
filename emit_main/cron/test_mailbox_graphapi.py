import argparse
import json
import requests
import time

from bs4 import BeautifulSoup

from emit_main.workflow.workflow_manager import WorkflowManager


#grabs the users access token to interact with inbox
def get_access_token(tenant_id, client_id, client_secret):
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default"
    }
    
    response = requests.post(url, data=payload)
    response_data = response.json()
    access_token = response_data['access_token']
    
    return access_token

#grabs a single message using the id of the message
#not sure if this is useful in this script since you can find the id by using retieve_emails
#which will grab ALL emails...and at that point you already 
def get_message(url, access_token, message_id):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.get(f"{url}/messages/{message_id}", headers=headers)
    response_data = response.json()

    print(response.json())
    return response_data

def mark_message_read(url, access_token, message_id):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "isRead": True
    }
    
    response = requests.patch(f"{url}/messages/{message_id}", headers=headers, json=payload)
    
    response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)

    if response.status_code == 200:
        print("Email marked as read successfully!")
        # print("Response:", response.json())
    elif response.status_code == 204:
        print("Email marked as read successfully (No Content).")
        
        
#grab all emails in inbox, use unread to toggle only grabbing unread messages
def retrieve_emails_bak(base_url, access_token, unread=True):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    url = base_url + "/messages"
    if unread:
        url += "?$filter=isRead ne true&$count=true"
        
    response = requests.get(url, headers=headers)
    
    
    if response.status_code == 200:
        graph_data = response.json()
        #print("Graph API output:")
        #print(json.dumps(graph_data, indent=4))
        messages = graph_data.get('value', [])
    
        for msg in messages:
            message_id = msg.get('id')
            subject = msg.get('subject')
            frm = msg.get('from', {}).get('emailAddress', {}).get('address')
            
            # preview = msg.get('bodyPreview') 
            body = msg.get('body', {}).get('content')
            
            print("Subject: {}\nFrom: {}\nBody: {}".format(subject, frm, body))
            
            print("Marking email as read")
            mark_message_read(base_url, access_token, message_id)
            
    else:
        print("Error:")
        print(response.get("error"))
        print(response.get("error_description"))
        print(response.get("correlation_id"))


def retrieve_emails(base_url, access_token):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    url = base_url + "/messages"
        
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        graph_data = response.json()
        # print("Graph API output:")
        # print(json.dumps(graph_data, indent=4))
        messages = graph_data.get('value', [])
        return messages
    else:
        print("Error:")
        print(response.get("error"))
        print(response.get("error_description"))
        print(response.get("correlation_id"))
        return []

def get_folder_id(url, access_token, folder_name):
    """
    Retrieves the ID of a folder by its name.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.get(f"{url}/mailFolders", headers=headers)
    response.raise_for_status()

    folders = response.json().get("value", [])
    for folder in folders:
        if folder.get("displayName") == folder_name:
            return folder.get("id")

    return None


def get_folder_id_by_path_segments(url, access_token, path_segments):
    """
    Retrieves the ID of a folder by its path segments.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.get(f"{url}/mailFolders", headers=headers)
    response.raise_for_status()

    folders = response.json().get("value", [])
    folder_id = None
    for folder in folders:
        if folder.get("displayName") == path_segments[0]:
            folder_id = folder.get("id")
            break

    if folder_id is not None and len(path_segments) > 1:
        # If there are more segments, we need to find the subfolder
        subfolder_name = path_segments[1]
        response = requests.get(f"{url}/mailFolders/{folder_id}/childFolders", headers=headers)
        response.raise_for_status()
        subfolders = response.json().get("value", [])
        for subfolder in subfolders:
            if subfolder.get("displayName") == subfolder_name:
                return subfolder.get("id")

    return folder_id


def move_message(url, access_token, message_id, destination_folder_id):
    """
    Moves a message to a different folder.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "destinationId": destination_folder_id
    }
    response = requests.post(f"{url}/messages/{message_id}/move", headers=headers, json=payload)
    response.raise_for_status()
    print("Message moved successfully!")

def send_email(base_url, access_token):
    url = base_url +"/sendMail"
    
    recipients = [
        "someone-to-spam-here@jpl.nasa.gov", 
        "mailing-list-to-spam-here@jpl.nasa.gov"
    ]
    
    to_recipients = build_recipients(recipients)
    
    subject = "Testing graph api sending emails"
    body = get_sample_body()

    msg_payload = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "text", # Can be "text" or "html"
                "content": body
            },
            "toRecipients": to_recipients,
            # Optional: Add CC, BCC
            # "ccRecipients": [
            #     {"emailAddress": {"address": "cc_recipient@example.com"}}
            # ],
            # "bccRecipients": [
            #     {"emailAddress": {"address": "bcc_recipient@example.com"}}
            # ],
            # Optional: Add attachments
            # "attachments": [
            #     {
            #         "@odata.type": "#microsoft.graph.fileAttachment",
            #         "name": "example.txt",
            #         "contentType": "text/plain",
            #         "contentBytes": base64.b64encode(b"This is attachment content.").decode('utf-8')
            #     },
            #     {
            #         "@odata.type": "#microsoft.graph.itemAttachment", # Attach another message
            #         "name": "attached_message.eml",
            #         "item": {
            #             "@odata.type": "#microsoft.graph.message",
            #             "subject": "Attached Test Message",
            #             "body": {
            #                 "contentType": "Text",
            #                 "content": "This is a message attached as an item."
            #             }
            #         }
            #     }
            # ]
        },
        "saveToSentItems": True # Set to True to save a copy in the sender's Sent Items folder
    }
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    print(f"Sending email from to '{recipients}'...")
    try:
        response = requests.post(url, headers=headers, data=json.dumps(msg_payload))

        # Check the response status code
        response.raise_for_status() # Raise an exception for 4xx or 5xx status codes

        if response.status_code == 202: # 202 Accepted indicates the request was accepted for processing
            print("Email sent successfully! Status: 202 Accepted")
        else:
            print(f"Unexpected status code: {response.status_code}")
            print(f"Response: {response.text}")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred: {req_err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        

def get_sample_body():
    body = str(time.time())
    body += """
You have my sword
You have my bow
and my axe
They're more like guidelines than actual rules
What say you to 3 shillings and we forget the name

"""
    
    return body

def build_recipients(recipients):
    to_recipients = []
    
    for rep in recipients:
        to_recipients.append({
            "emailAddress": {
                "address": rep
            }
        })
    
    return to_recipients
        

def main():

    # Set up args
    parser = argparse.ArgumentParser(description="Generate a daily report")
    parser.add_argument("-e", "--env", default="test", help="Where to run the report")
    args = parser.parse_args()

    env = args.env
    # Get workflow manager and db collections
    config_path = f"/store/emit/{env}/repos/emit-main/emit_main/config/{env}_sds_config.json"
    wm = WorkflowManager(config_path=config_path)
    base_url = f"{wm.config['graph_api_base_url']}{wm.config['email_user']}"
    access_token = get_access_token(wm.config['graph_api_tenant_id'], wm.config['graph_api_app_id'], wm.config['graph_api_secret'])
    
    # grab all the unread emails
    messages = retrieve_emails(base_url, access_token)
    delivery_response_success_folder_id = get_folder_id_by_path_segments(base_url, access_token, ["EMIT Delivery Responses", env.capitalize(), "Success"])
    delivery_response_failure_folder_id = get_folder_id_by_path_segments(base_url, access_token, ["EMIT Delivery Responses", env.capitalize(), "Failure"])
    print(f"Delivery Response Success Folder ID: {delivery_response_success_folder_id}")
    print(f"Delivery Response Failure Folder ID: {delivery_response_failure_folder_id}")
    for m in messages:
        message_id = m.get('id')
        subject = m.get('subject')
        frm = m.get('from', {}).get('emailAddress', {}).get('address')
        time_received = m.get('receivedDateTime')

        # preview = msg.get('bodyPreview') 
        body = m.get('body', {}).get('content')
        content_type = m.get('body', {}).get('contentType')
        if content_type == "html":
            # Convert HTML to plain text
            soup = BeautifulSoup(body, "html.parser")
            body_text = soup.get_text().lstrip()
        else:
            body_text = body

        # Check that body starts with "{" to indicate a JSON response
        if not body_text.startswith("{"):
            print(f"Unable to find JSON at start of message with subject \"{subject}\" dated "
                        f"{time_received}. Leaving in inbox.")
            print(body_text)
            continue
        else:
            print("Looks like JSON!")

        # print("Subject: {}\nFrom: {}\nBody: {}".format(subject, frm, body_text))
        
        # print("Moving email to appropriate folders")
        # move_message(base_url, access_token, message_id, archive_folder_id)

if __name__ == "__main__":
    main()
