import json

class MessageBox():
    """
    Message box (message) transferred from a DataReader to a DataWriter

    Attributes:
        data (object): stores data itself (TO DO enforce a list of dictionaries)
        metadata (object): stores the package metadata

    Methods:
        get_data(): returns the stored data
        get_metadata(): returns the package metadata
    """

    def __init__(self, data: object, metadata: object):
        """
        Initializes the MessageBox instance

        Args:
            data (object): data to be stored (TO DO enforce a list of dictionaries)
            metadata (object): package metadata 
        """
        self.data = data
        self.metadata = metadata

    def get_data(self) -> object:
        """
        Returns the stored data

        Returns:
            object: stored data
        """
        return self.data

    def get_metadata(self) -> object:
        """
        Returns the package metadata

        Returns:
            object: package metadata
        """
        return self.metadata

    def get_json(self) -> object:
        """
        Returns the Message box in json format:
        { metadata : stuff, data : stuff }

        Returns:
            object: Message box in json format
        """

        return json.JSONDecoder().decode(json.dumps(
            { 'metadata' : self.metadata, 'data': self.data}
        ))