import { Utility } from "../../src/utility/utility";
import { Core } from "nodets-ms-core";

jest.mock("nodets-ms-core", () => ({
    Core: {
        getStorageClient: jest.fn()
    }
}));

describe("Utility.waitForBlobAvailability", () => {
    const tdei_dataset_id = "test-dataset";
    const remoteUrl = "https://example.com/file.txt";

    beforeEach(() => {
        jest.clearAllMocks();
        jest.spyOn(global, "setTimeout").mockImplementation((cb: (...args: any[]) => void, ms?: number, ...args: any[]): NodeJS.Timeout => {
            cb(...args);
            // Return a dummy object to satisfy NodeJS.Timeout type
            return {} as NodeJS.Timeout;
        });
        jest.spyOn(Utility, "sleep").mockImplementation(() => Promise.resolve());
    });

    afterEach(() => {
        jest.restoreAllMocks();
    });

    it("should resolve immediately if file is available on first try", async () => {
        const mockGetFileFromUrl = jest.fn().mockResolvedValue({ fileName: "file.txt" });
        (Core.getStorageClient as jest.Mock).mockReturnValue({
            getFileFromUrl: mockGetFileFromUrl
        });
        const logSpy = jest.spyOn(console, "log").mockImplementation(() => { });

        await expect(Utility.waitForBlobAvailability(tdei_dataset_id, remoteUrl)).resolves.toBeUndefined();

        expect(Core.getStorageClient).toHaveBeenCalled();
        expect(mockGetFileFromUrl).toHaveBeenCalledWith(remoteUrl);
        expect(logSpy).toHaveBeenCalledWith(
            expect.stringContaining(`File for dataset ${tdei_dataset_id} is available in Azure Blob Storage. File name: file.txt`)
        );
    });

    it("should retry if file is not available and succeed on second try", async () => {
        const mockGetFileFromUrl = jest
            .fn()
            .mockRejectedValueOnce(new Error("Not found"))
            .mockResolvedValueOnce({ fileName: "file.txt" });
        (Core.getStorageClient as jest.Mock).mockReturnValue({
            getFileFromUrl: mockGetFileFromUrl
        });
        const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => { });
        const logSpy = jest.spyOn(console, "log").mockImplementation(() => { });

        await expect(Utility.waitForBlobAvailability(tdei_dataset_id, remoteUrl)).resolves.toBeUndefined();

        expect(mockGetFileFromUrl).toHaveBeenCalledTimes(2);
        expect(warnSpy).toHaveBeenCalledWith(
            expect.stringContaining("File not available. Retry 1/2 in 5 seconds...")
        );
        expect(logSpy).toHaveBeenCalledWith(
            expect.stringContaining(`File for dataset ${tdei_dataset_id} is available in Azure Blob Storage. File name: file.txt`)
        );
    });

    it("should retry up to maxRetries and log error if file is not found", async () => {
        const mockGetFileFromUrl = jest.fn().mockRejectedValue(new Error("Not found"));
        (Core.getStorageClient as jest.Mock).mockReturnValue({
            getFileFromUrl: mockGetFileFromUrl
        });
        const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => { });
        const errorSpy = jest.spyOn(console, "error").mockImplementation(() => { });

        await Utility.waitForBlobAvailability(tdei_dataset_id, remoteUrl);

        expect(mockGetFileFromUrl).toHaveBeenCalledTimes(2);
        expect(warnSpy).toHaveBeenCalledWith(
            expect.stringContaining("File not available. Retry 1/2 in 5 seconds...")
        );
        expect(warnSpy).toHaveBeenCalledWith(
            expect.stringContaining("File not available. Retry 2/2 in 5 seconds...")
        );
        expect(errorSpy).toHaveBeenCalledWith(
            expect.stringContaining(`File for dataset ${tdei_dataset_id} not found after 2 retries.`)
        );
    });
});