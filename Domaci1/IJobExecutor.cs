namespace Domaci1;

// interface, a ne DefaultJobExecutor, zbog testova -
// mozemo ubaciti mock bez da cekamo da se stvarno izvrsi
public interface IJobExecutor
{
    int Execute(Job job);
}
