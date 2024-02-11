

from telethon import TelegramClient, events, sync, client, errors
from dotenv import dotenv_values
import telethon
from telethon.tl.functions.channels import JoinChannelRequest
from dataclasses import  dataclass
import pandas as pd

parameters = dotenv_values("../.env")

@dataclass
class TelegramBot:
    client: TelegramClient
    channel_req: JoinChannelRequest
    main_df: pd.DataFrame
    
    def __init__(self, channel_message):
        self.client = TelegramClient(session='covid_19_data_parsing', api_id=parameters['API_ID'], api_hash=parameters['API_HASH'] )
        self.client.start()
        self.main_df = pd.DataFrame(columns=["channel_name", "user_id", "message_text", "media_messages", "is_reply", "mentions", "metadata"])
        
    async def _connect_channel(channel:str):
        """
        this function connects to the channel.
        channel: is the name of the channel for which you want to connect.
        """
        try:
            await client(JoinChannelRequest(channel))
        except errors.RPCError as e:
            print("not able to connect channel, the error is: ", e) 
    
    async def fetch_channels(self) -> list:
        """
        function fetches all of the defined dataset based on the channels.
        """ 
        subscribed_channels = []       
        try:
            for dialog in self.client.iter_dialogs():
                if dialog.is_channel:
                    subscribed_channels.append(dialog.title)
        except Exception as e:
            print("in the function fetch_channels, the error is: ", e)
        return subscribed_channels
    
    async def fetch_my_messages(self, message_limit):
        """
        Fetches the history of the messages for the current user(whose personal crednetials are used).
        This is case user wants to run the RAG queries with his / her data. 
        this function fetches the messages wrt the channels and stores them in the dataframe.
        message_limit: is the number of messages you want to fetch.
        """
        
        try:
            ## get all of the channels the user is subscribed
            message = {}
            media_path = ""
            counter = 0
            entities = await self.client.get_messages(limit=message_limit)
            for entity in entities:
                if isinstance(entity, telethon.types.Message):
                    if  entity.media == True:
                        entity.download_media()
                        media_path =  f"{entity.grouped_id}/{entity.post_author}/{entity.id}"
                        with open(media_path, "wb") as f:
                            f.write(media_path)
                        print(f"media downloaded by {entity.post_author} at location: " + media_path) 
                    message.append({
                            "channel_name": entity.id,
                            "message_text": entity.message,
                            "media_messages": entity.media,
                            "reply_to": entity.reply_to,
                            "media_path": media_path  if media_path else ''
                                })
                    counter += 1
                if counter == message_limit:
                    break                     
        except Exception as e:
            print("in the function fetch_my_messages, the error is: ", e)
        return pd.DataFrame.from_dict(data=message)
        
    async def fetch_messages_all(self,channel:str, message_limit):
        """
        this function fetches all of the messages from the given channel with the parameter of the number of messages.
        channel: is the name of the channel for which you want to fetch the messages.
        message_limit: is the number of messages you want to fetch.
        """
        try:
            await self._connect_channel(channel) 
            messages = self.client.get_messages(channel,limit=message_limit)  
            message_texts = [message.text for message in messages]
            return message_texts
        
        except Exception as e:
            print("in the function integrating_channels, the error is defined here: ", e)
        

    async def finish_session(self):
        self.client.disconnect()
        

