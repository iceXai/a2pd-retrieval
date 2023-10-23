
# In[]
from job import RetrievalJob
from clargparser import CommandLineArgParser

# In[]

def retrieval_job_processor() -> None:
    """
    Hanlder for the command-line-based execution of the swath retrieval tool
    """
    #initialize argument parser    
    clarg = CommandLineArgParser()
    #parse arguments
    clarg.parse_command_line_arguments()
    #return arguments
    args = clarg.get_args()

    #initialize a retrieval job
    job = RetrievalJob(args)
    #validate arguments and setting files    
    job.validate()
    #setup the job processor
    job.setup()
    #exectue it
    job.run()



# In[]        
if __name__ == "__main__":
    retrieval_job_processor()  