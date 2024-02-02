#ifndef RIDH
#define RIDH

struct RID
{
    RID()
    {
        std::cout << "Initialize RID" << std::endl;
    }
    ~RID()
    {
        std::cout << "Delete RID" << std::endl;
    }
    RID(const RID&)
    {
        std::cout << "Copy RID" << std::endl;
    }
};

#endif
