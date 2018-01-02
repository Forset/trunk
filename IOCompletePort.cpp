// IOCompletePort.cpp : Defines the entry point for the console application.
//
#include "stdafx.h"
#include <Windows.h>
#include <iostream>

enum
{
    CK_READ = 1,
    CK_WRITE,
};

void ShowErrorMsg(const std::wstring& strMsg, int nError);

int wmain(int argc, wchar_t* argv[])
{
    std::wstring strCmd;
    if (argc >= 2)
        strCmd = argv[1];

    std::wstring strSrcFile;
    if (!strCmd.empty() && 0 == wcsncmp(strCmd.c_str(), L"-file=", 6))
        strSrcFile = strCmd.substr(6);

    if (strSrcFile.empty())
        return -1;

    std::wstring strDestFile = strSrcFile + L"_copy";

    std::cout << "Begin to open source file\n";

    HANDLE hSrcFile = ::CreateFile(
        strSrcFile.c_str(),                                        //Դ�ļ�
        GENERIC_READ,                                  //��ģʽ
        FILE_SHARE_READ,                              //������
        NULL,                                         //��ȫ����
        OPEN_EXISTING,                                  //�������
        FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING, //�첽 | ���û���
        NULL                                          //�ļ�ģ��Ϊ��
        );

    if (INVALID_HANDLE_VALUE == hSrcFile)
    {
        ShowErrorMsg(L"Failure to open source file", -1);
        return -1;
    }

    std::cout << "Begin to open dest file\n";

    HANDLE hDestFile = ::CreateFile(
        strDestFile.c_str(),                                        //Ŀ���ļ�
        GENERIC_WRITE,                                 //дģʽ
        FILE_SHARE_READ,                               //������
        NULL,                                           //��ȫ����
        CREATE_ALWAYS,                                   //���Ǵ���
        FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING, //�첽 | ���û���
        hSrcFile                                       //�ļ�����ͬԴ�ļ�
        );

    if (INVALID_HANDLE_VALUE == hDestFile)
    {
        ShowErrorMsg(L"Failure to open dest file", -1);
        return -1;
    }

    LARGE_INTEGER liFileSize = { 0 };
    BOOL bRet = ::GetFileSizeEx(hSrcFile, &liFileSize);
    if (FALSE == bRet)
    {
        ShowErrorMsg(L"Failure to get size of source file", bRet);
        return -1;
    }

    if (FALSE == ::SetFilePointerEx(hDestFile, liFileSize, NULL, FILE_BEGIN)
        || FALSE == ::SetEndOfFile(hDestFile))
    {
        ShowErrorMsg(L"Failure to set size of dest file", 0);
        return -1;
    }

    DWORD dwBytesPerSector = 0;
    bRet = ::GetDiskFreeSpace(strSrcFile.substr(0, 2).c_str(), NULL, &dwBytesPerSector, NULL, NULL);
    if (FALSE == bRet)
    {
        ShowErrorMsg(L"Failure to get size of disk", bRet);
        return -1;
    }


    SYSTEM_INFO sysInfo = { 0 };
    ::GetSystemInfo(&sysInfo);

    HANDLE hIOCP = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, sysInfo.dwNumberOfProcessors);
    if (NULL == hIOCP)
    {
        DWORD dwErr = ::GetLastError();
        if (dwErr != ERROR_ALREADY_EXISTS)
        {
            ShowErrorMsg(L"Failure to create IO Completion Port", dwErr);
            return -1;
        }
    }

    OVERLAPPED ovlpRead = { 0 };
    hIOCP = ::CreateIoCompletionPort(hSrcFile, hIOCP, CK_READ, sysInfo.dwNumberOfProcessors);

    OVERLAPPED ovlpWrite = { 0 };
    hIOCP = ::CreateIoCompletionPort(hDestFile, hIOCP, CK_WRITE, sysInfo.dwNumberOfProcessors);

    size_t sizeMAX = dwBytesPerSector * 1024 * 64 * 2; //512K * 64 * 2
    size_t sizeMIN = dwBytesPerSector * 1024 * 64 * 2;

    LPVOID lpAddr = ::VirtualAlloc(NULL, sizeMAX, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);
    if (NULL == lpAddr)
    {
        ShowErrorMsg(L"Failure to allocate memory", 0);
        return -1;
    }


    //����IOCP����ɶ��в���һ��д0�ֽ������
    ::PostQueuedCompletionStatus(
        hIOCP,     //IOCP
        0,           //GetQueuedCompletionStatusȡ���Ĵ����ֽ�Ϊ0
        CK_WRITE,  //д����
        &ovlpWrite //дOVERLAPPED
        );


    /************************************************************************
    ��ÿ���������������ʱ���ѻ������е�����д�롾Ŀ���ļ����������¡�Դ�ļ�����ƫ����

    ��ÿ����д���������ʱ�����¡�Ŀ���ļ�����ƫ������
    ͬʱ����Ϊ����������д�����ں����д������ɺ󣬸��ݸ��º�ġ�Դ�ļ�����ƫ����
    �͡�Դ�ļ�����С���Ƚϣ�������ڵ���Դ�ļ���С����˵���������һ�ζ�ȡ����������һ��
    д�������ʱ �˳�ѭ���� �����ǰ��Դ�ļ�ƫ������û�дﵽ��Դ�ļ���С�����ٴδӡ�Դ�ļ���
    �ж�ȡ���ݽ���������
    ************************************************************************/
    while (1)
    {
        DWORD dwBytesTrans = 0;
        ULONG_PTR ulCompleteKey = 0;
        LPOVERLAPPED lpOverlapped = NULL;
        BOOL bRet = ::GetQueuedCompletionStatus(hIOCP, &dwBytesTrans, &ulCompleteKey, &lpOverlapped, INFINITE);
        if (FALSE == bRet)
        {
            DWORD dwErr = ::GetLastError();
            if (NULL != lpOverlapped)
            {
                ShowErrorMsg(L"Thread function error:", dwErr);
                break;
            }
            else
            {
                if (ERROR_TIMEOUT == dwErr)
                    ShowErrorMsg(L"GetQueuedCompletionStatus timeout", dwErr);
                else
                    ShowErrorMsg(L"Thread function error:", dwErr);
                continue;
            }
        }

        static LARGE_INTEGER liSrcFile = { 0 };
        static LARGE_INTEGER liDestFile = { 0 };
        static int iRead = 0;
        static int iWrite = 0;

        if (CK_READ == ulCompleteKey) //��������� 
        {
            liSrcFile.QuadPart += dwBytesTrans;
            ovlpRead.Offset = liSrcFile.LowPart;
            ovlpRead.OffsetHigh = liSrcFile.HighPart;

            std::cout << std::endl << "-------------Index " << ++iRead << " Read completion, Begin to write file ---------------- " << std::endl;
            ::WriteFile(hDestFile, lpAddr, sizeMIN, NULL, &ovlpWrite);
        }
        else if (CK_WRITE == ulCompleteKey) //д������� 
        {
            liDestFile.QuadPart += dwBytesTrans;
            ovlpWrite.Offset = liDestFile.LowPart;
            ovlpWrite.OffsetHigh = liDestFile.HighPart;

            if (liSrcFile.QuadPart >= liFileSize.QuadPart)
                break;

            std::cout << std::endl << "*************Index " << ++iWrite << " Write completion, Begin to read file***************" << std::endl;
            ::ReadFile(hSrcFile, lpAddr, sizeMIN, NULL, &ovlpRead);
        }
    }

    ::SetFilePointerEx(hDestFile, liFileSize, NULL, FILE_BEGIN);
    ::SetEndOfFile(hDestFile);

    std::cout << std::endl << " Copy Finished\n";

    if (INVALID_HANDLE_VALUE != hSrcFile)
    {
        ::CloseHandle(hSrcFile);
        hSrcFile = INVALID_HANDLE_VALUE;
    }

    if (INVALID_HANDLE_VALUE != hDestFile)
    {
        ::CloseHandle(hDestFile);
        hDestFile = INVALID_HANDLE_VALUE;
    }

    if (NULL != lpAddr)
    {
        ::VirtualFree(lpAddr, 0, MEM_RELEASE | MEM_DECOMMIT);
        lpAddr = NULL;
    }

    return 0;
}

void ShowErrorMsg(const std::wstring& strMsg, int nError)
{
    std::wcout << std::endl << strMsg.c_str() << L" " << nError << std::endl;
}
