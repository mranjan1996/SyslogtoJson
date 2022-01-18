class SensitiveWordsException(ValueError):
    '''
    Raised when the number of arguments supplied to a function is incorrect.
    Note that this check is only performed from the number of arguments
    specified in the validate_accept() decorator. If the validate_accept()
    call is incorrect, it is possible to have a valid function where this
    will report a false validation.
    '''

    def __init__(self, func_name, file_path):
        self.error = 'File not available for {0} - (1)'.format(func_name, file_path)

    def __str__(self):
        return self.error